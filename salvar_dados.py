import selenium as selenium
from selenium import webdriver
import csv
from selenium.webdriver.common.keys import Keys
import pyautogui
import os

# lista de todas as estações
arquivo_aberto = open('lista_codigo_estacoes.csv', 'rt')
leitura = csv.reader(arquivo_aberto, delimiter=';')

options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument("--test-type")
options.binary_location = "/usr/bin/chromium"
driver = selenium.webdriver.Chrome(executable_path="C:\Projetos Python\WebDrive\chromedriver.exe")

driver.get('http://www.inmet.gov.br/projetos/rede/pesquisa/inicio.php')
digitar_usuario = driver.find_element_by_name('mCod')
digitar_usuario.send_keys('meu_email')
digitar_senha = driver.find_element_by_name('mSenha')
pyautogui.PAUSE = 1.5
digitar_senha.send_keys('minha_senha')
pyautogui.PAUSE = 1.5
digitar_senha.send_keys(Keys.ENTER)
serie_historica = driver.find_element_by_link_text('Série Histórica - Dados Diários').click()

for estacoes in leitura:
    try:
        url_pesquisa1 = 'http://www.inmet.gov.br/projetos/rede/pesquisa/gera_serie_txt.php?&mRelEstacao='
        estacao = str(estacoes[2])
        url_pesquisa2 = '&btnProcesso=serie&mRelDtInicio=01/01/1960&mRelDtFim=17/10/2018&mAtributos=,,1,1,,,,,,1,1,,' \
                        '1,1,1,1, '

        driver.get(url_pesquisa1 + estacao + url_pesquisa2)
        pyautogui.hotkey('ctrl', 's')
        pyautogui.hotkey('home')
        pyautogui.typewrite('C:\Projetos Python\Web_crawler-inmet.gov.br-master\Arquivo' + os.sep)
        pyautogui.hotkey('tab')
        pyautogui.press(['down', 'down', 'up'])
        pyautogui.hotkey('enter')
        pyautogui.hotkey('enter')
    except:
        driver.quit()
driver.quit()
