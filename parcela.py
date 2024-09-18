import pandas as pd
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import matplotlib.pyplot as plt
import requests
import paho.mqtt.client as mqtt

# Configurações do ThingSpeak e MQTT
THINGSPEAK_API_KEY = 'O5IVRTNTVVKDCGVM'
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "meu/topico"
MQTT_ACCESS_TOKEN = "2ODUALSYQ2319V73"  

# conexão MQTT
def on_connect(client, userdata, flags, rc):
    print(f"Conectado ao broker MQTT com código {rc}")

def on_publish(client, userdata, mid):
    print("Mensagem publicada")

# Iiniciar MQTT
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_publish = on_publish
mqtt_client.username_pw_set(MQTT_ACCESS_TOKEN)
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

# enviar dados ao ThingSpeak
def send_to_thingspeak(api_key, field1, field2, field3, field4, field5):
    url = f"https://api.thingspeak.com/update?api_key={api_key}&field1={field1}&field2={field2}&field3={field3}&field4={field4}&field5={field5}"
    response = requests.get(url)
    if response.status_code == 200:
        print("Dados enviados com sucesso ao ThingSpeak!")
    else:
        print("Falha ao enviar dados ao ThingSpeak.")

# browser no site do WhatsApp
navegador = webdriver.Chrome()
navegador.get('https://web.whatsapp.com/')

# aguarde até que seja carregado
while len(navegador.find_elements(By.ID, "side")) < 1:
    time.sleep(1)
time.sleep(3)

# planilha que será usada para envio de mensagens
df = pd.read_excel("Primeira parcela.xlsx")

data = df['Vencimento'].unique()
# converter a data para dia, mês e ano
df_date = pd.DataFrame({'Vencimento': data})
df_date['Vencimento'] = pd.to_datetime(df_date['Vencimento'])
df_date['Vencimento_formatado'] = df_date['Vencimento'].dt.strftime('%d-%m-%Y')
df['Vencimento'] = df['Vencimento'].map(df_date.set_index('Vencimento')['Vencimento_formatado'])

# substituir linhas vazias por zero
df.fillna(0, inplace=True)

print(df.info())

def alt_telefone(numero):
    if isinstance(numero, str):
        return numero.replace('-', '').replace(' ', '').replace('(', '').replace(')', '')
    return numero

df['Telefone'] = df['Telefone'].apply(alt_telefone)

# contabilizar as formas de pagamento, seguradoras e consultores
forma_pgto = df['Forma de pagamento'].value_counts()
seguradora_mais_escolhida = df['Seguradora'].value_counts().idxmax()
consultor_mais_preencheu = df['Consultor'].value_counts().idxmax()

# resumo ThingSpeak
send_to_thingspeak(THINGSPEAK_API_KEY, forma_pgto.idxmax(), seguradora_mais_escolhida, consultor_mais_preencheu, len(df), None)

# gráfico com a forma de pagamento mais utilizada pelos clientes
plt.figure(figsize=(8, 6))  
plt.pie(forma_pgto, labels=forma_pgto.index, autopct='%1.0f%%', startangle=75)
plt.title('Distribuição de Formas de Pagamento')
plt.axis('equal')
plt.show()


for linha in df.itertuples():
    nome = linha[1]
    vcto = linha[2]
    pgto = linha[3]
    seguradora = linha[4]
    telefone = linha[5]
    telefone = int(telefone)
    consultor = linha[6]
    print(telefone)
    try:
        # utilizar os dados da planilha para o envio das mensagens
        msg = f"Olá {nome}, venho através desta mensagem lembrar que a parcela de seu seguro {seguradora} vencerá em {vcto} através da forma de pagamento: {pgto}, caso tenha alguma dúvida entrar em contato com a(o) especialista de seguros que lhe atendeu, {consultor}."
        link_mensagem_whatsapp = f"https://web.whatsapp.com/send?phone=55{telefone}&text={quote(msg)}"
        time.sleep(1)
        # digitar número informado e escrever a mensagem após abrir o navegador
        navegador.get(link_mensagem_whatsapp)

        while len(navegador.find_elements(By.ID, "side")) < 1:
            time.sleep(1)
        time.sleep(14)

        navegador.find_element(By.XPATH, '//*[@id="main"]/footer/div[1]/div/span[2]/div/div[2]/div[2]/button/span').click()
        time.sleep(3)

        # enviar dados para o ThingsBoard via MQTT
        payload = {
            "nome": nome,
            "vencimento": vcto,
            "pagamento": pgto,
            "seguradora": seguradora,
            "telefone": telefone,
            "consultor": consultor
        }
        mqtt_client.publish(f"v1/devices/{2658618}/telemetry", str(payload))

        # enviar dados para o ThingSpeak
        send_to_thingspeak(THINGSPEAK_API_KEY, pgto,seguradora, consultor, None, None, None)

    except ValueError as e:
        print(f'Erro ao enviar mensagem para {nome}:{e}')
        with open('erros.txt', 'a', newline='', encoding='utf-8') as arquivo:
            arquivo.write(f'Erro ao enviar mensagem para {nome}\n')

    except Exception as e:
        print(f'Erro ao enviar mensagem para {nome}')
        with open('erros.txt', 'a', newline='', encoding='utf-8') as arquivo:
            arquivo.write(f'Erro ao enviar mensagem para {nome}\n')

navegador.quit()

mqtt_client.loop_stop()