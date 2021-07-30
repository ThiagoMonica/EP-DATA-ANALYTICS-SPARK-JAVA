from pyspark.context import SparkContext
from pyspark.sql import column
from pyspark.sql.session import SparkSession
from py4j.java_gateway import JavaGateway
import matplotlib.pyplot as plt


# A implementar:
# [V] demais variaveis presentes nos aquivos (por enquanto so tem 2 DEWP e TEMP)
# [V] Filtro de intervalo de datas na tabela a ser filtrada
# [V] Grafico todo maluco ( isso fudeu pq eu n faco a menor ideia)
# [] Como o resultado sera agrupado ( dias, meses, anos) ?
# [] Mais formas de calculos estatisticos - opcional (se sobrar tempo)


sc = SparkContext('local')
spark = SparkSession(sc)

# Configuracao para permitir o input no pyspark
scanner = sc._gateway.jvm.java.util.Scanner  
sys_in = getattr(sc._gateway.jvm.java.lang.System, 'in')

# Leitura do arquivo
file_location = "./TabelaDSID.csv"
file_type = "csv"
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

def menuInformacao():
	i = -1
	for col in df.dtypes:
		i=i+1
		print(str(i) + " \t " + col[0] + " (" + col[1] + ")")

# Funcao para saber o nome do tipo da informacao escolhido
def tipoInformacao(numero):
	return df.schema.names[int(numero)]

def calculaMedia(tabela, coluna):
	return tabela.groupBy().sum(coluna).collect()[0][0] / tabela.count()

def calculaDP(tabela, coluna):
	media = calculaMedia(tabela, coluna)
	variavelTipo = "tabela." + coluna

	tabelaFill = tabela.select(tabela.DATE, eval(variavelTipo), ((eval(variavelTipo) - media)*(eval(variavelTipo) - media)).alias('aux'))
	return (tabelaFill.groupBy().sum("aux").collect()[0][0]/ (tabelaFiltrada.count()-1)) ** 0.5

def calculaQuadradosMin(tabela, x, y):
	mediax = calculaMedia(tabela, x)
	mediay = calculaMedia(tabela, y)

	#variavelTipo1 = "%a.%b" % (tabela, x)
	variavelTipo1 = "tabela." + x
	variavelTipo2 = "tabela." + y

	tabelaCalc = tabela.select(eval(variavelTipo1), (eval(variavelTipo1)*(eval(variavelTipo2)-mediay)).alias('aux = xi(yi-mediay)'), eval(variavelTipo2), (eval(variavelTipo1)*(eval(variavelTipo1)-mediax)).alias('aux = xi(xi-mediax)'))
	b = tabelaCalc.groupBy().sum("aux = xi(yi-mediay)").collect()[0][0] / tabelaCalc.groupBy().sum("aux = xi(xi-mediax)").collect()[0][0]
	a = mediay - b * mediax
	
	y0 = a + b*(tabela.groupBy().min(x).collect()[0][0])
	y1 = a + b*(tabela.groupBy().max(x).collect()[0][0])
	return a,b,y0,y1

def plotGrafico(tabela, coluna):
	print("Desejar vizualizar as estatisticas da primeira informacao no decorrer do tempo selecionado?")
	print("1 \t Sim")
	print("2 \t Nao")
	print("Numero escolhido:")
	escolhaGrafico = scanner(sys_in).nextLine()

	if(escolhaGrafico=="1"):
		pandasDF = tabela.toPandas()

		plt.figure()
		plt.plot(pandasDF['DATE'], pandasDF[coluna], 'g', label = "TEMP")
		plt.axhline(y=pandasDF[coluna].mean(), color="red", label="Media")
		plt.axhspan(pandasDF[coluna].mean() - pandasDF[coluna].std(), pandasDF[coluna].mean() + pandasDF[coluna].std(), facecolor='lavender', alpha=0.5)
		plt.xlabel('DATE')
		plt.ylabel(coluna)
		plt.show()
		return


while (True):
	print("\n\n******** MENU ********")
	print("Digite numeros de 1 a 5 para escolher o metodo de calculo\n")
	print("1 \t Media")
	print("2 \t Desvio Padrao")
	print("3 \t Metodo dos quadrados minimos")
	print("4 \t Para Sair")

	metodoCalculo = scanner(sys_in).nextLine()

	# Encerra o programa ao digitar a opcao 4 (sair)
	if(metodoCalculo == "4"):
		exit()

	print("Escolha o tipo de informacao que deseja utilizar para os calculos")
	menuInformacao()

	tipoDaInfor01 = scanner(sys_in).nextLine()


	condition = True
	while(condition):
		print("Digite o intervalo de datas que sera utilizado para os calculos no formato aaaa-mm-dd")
		print("Por exemplo 20/05/2021 deverá ser escrito como 2021-05-20")
		print("Digite o limite superior (data maior): ")
		dataMaior = scanner(sys_in).nextLine()

		print("Digite o limite inferior (data menor): ")
		dataMenor = scanner(sys_in).nextLine()

		if(dataMaior>dataMenor):
			condition = False
		else:
			print("Você digitou um limite inferior maior que o limite superior ")
			print("Tente novamente \n\n")

	tabelaFiltrada = df.filter((df.DATE >= dataMenor) & (df.DATE <= dataMaior))

	if(metodoCalculo == "1"):
		nomeTipo = tipoInformacao(tipoDaInfor01)
		resultado = calculaMedia(tabelaFiltrada, nomeTipo)
		print("\n\nO resultado da media para o atributo", nomeTipo, "foi: \n", resultado, "\n")

		plotGrafico(tabelaFiltrada, nomeTipo)

	elif(metodoCalculo == "2"):
		nomeTipo = tipoInformacao(tipoDaInfor01)
		resultado = calculaDP(tabelaFiltrada, nomeTipo)
		print("\n\nO resultado do desvio padrao para o atributo", nomeTipo, "foi: \n", resultado, "\n")

		plotGrafico(tabelaFiltrada, nomeTipo)

	elif(metodoCalculo == "3"):
		print("Digite o segundo tipo de informacao")
		menuInformacao()
		tipoDaInfor02 = scanner(sys_in).nextLine()
		nomeTipo1 = tipoInformacao(tipoDaInfor01)
		nomeTipo2 = tipoInformacao(tipoDaInfor02)

		a,b,y0,y1 = calculaQuadradosMin(tabelaFiltrada, nomeTipo1, nomeTipo2)

		print("\n\nValor b: ", b)
		print("\n\nValor a:", a)
		print("\n\nA reta y0 tem valor: ", y0)
		print("\n\nA reta y1 tem valor: ", y1)

		pandasDF = tabelaFiltrada.toPandas()

		plt.figure()
		plt.plot(pandasDF[nomeTipo1], a + (b*pandasDF[nomeTipo1]), 'g', label = "fitted curve")
		plt.axhline(y=y0, color="red", linestyle="--", label="Y0")
		plt.axhline(y=y1, color="red", linestyle="--", label="Y1")
		plt.scatter(pandasDF[nomeTipo1], pandasDF[nomeTipo2], label = "data")
		plt.xlabel(nomeTipo1)
		plt.ylabel(nomeTipo2)
		plt.show()

		plotGrafico(tabelaFiltrada, nomeTipo1)
	else:
		print("Codigo não encontrado, por favor tente novamente!")
		continue
