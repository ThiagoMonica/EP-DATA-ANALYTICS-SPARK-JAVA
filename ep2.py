from pyspark.context import SparkContext
from pyspark.sql import column
from pyspark.sql.session import SparkSession
from py4j.java_gateway import JavaGateway


# A implementar:
# [V] demais variaveis presentes nos aquivos (por enquanto so tem 2 DEWP e TEMP)
# [V] Filtro de intervalo de datas na tabela a ser filtrada
# [] Grafico todo maluco ( isso fudeu pq eu n faco a menor ideia)
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
	#print(df.schema.names[2])
	i = -1
	for col in df.dtypes:
		i=i+1
		print(str(i) + " \t " + col[0] + " (" + col[1] + ")")

# Funcao para saber o nome do tipo da informacao escolhido
def tipoInformacao(numero):
  return df.schema.names[int(numero)]

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
		resultado = tabelaFiltrada.groupBy().sum(nomeTipo).collect()[0][0] / tabelaFiltrada.count()
		print("\n\nO resultado da media para o atributo", nomeTipo, "foi: \n", resultado, "\n")

	elif(metodoCalculo == "2"):
		nomeTipo = tipoInformacao(tipoDaInfor01)
		media = tabelaFiltrada.groupBy().sum(nomeTipo).collect()[0][0] / tabelaFiltrada.count()

		# Campo do tipo que sera utilizado
		variavelTipo = "tabelaFiltrada." + nomeTipo
		tabelaFill = tabelaFiltrada.select(tabelaFiltrada.DATE, eval(variavelTipo), ((eval(variavelTipo) - media)*(eval(variavelTipo) - media)).alias('aux'))
		resultado = (tabelaFill.groupBy().sum("aux").collect()[0][0]/ (tabelaFiltrada.count()-1)) ** 0.5
		print("\n\nO resultado do desvio padrao para o atributo", nomeTipo, "foi: \n", resultado, "\n")

	elif(metodoCalculo == "3"):
		print("Digite o segundo tipo de informacao")
		menuInformacao()
		tipoDaInfor02 = scanner(sys_in).nextLine()
		nomeTipo1 = tipoInformacao(tipoDaInfor01)
		nomeTipo2 = tipoInformacao(tipoDaInfor02)

		mediax = tabelaFiltrada.groupBy().sum(nomeTipo1).collect()[0][0] / tabelaFiltrada.count() 
		mediay = tabelaFiltrada.groupBy().sum(nomeTipo2).collect()[0][0] / tabelaFiltrada.count() 

		variavelTipo1 = "tabelaFiltrada." + nomeTipo1
		variavelTipo2 = "tabelaFiltrada." + nomeTipo2

		tabelaCalc = tabelaFiltrada.select(eval(variavelTipo1), (eval(variavelTipo1)*(eval(variavelTipo2)-mediay)).alias('aux = xi(yi-mediay)'), eval(variavelTipo2), (eval(variavelTipo1)*(eval(variavelTipo1)-mediax)).alias('aux = xi(xi-mediax)'))
		b = tabelaCalc.groupBy().sum("aux = xi(yi-mediay)").collect()[0][0] / tabelaCalc.groupBy().sum("aux = xi(xi-mediax)").collect()[0][0]
		a = mediay - b * mediax


		print("\n\nO valor da variavel b calculada é: ",b)
		print("\n\nO valor da variavel a calculada é: ",a)


	else:
		print("Codigo não encontrado, por favor tente novamente!")
		continue
