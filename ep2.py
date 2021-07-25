from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from py4j.java_gateway import JavaGateway


# A implementar:
# [] demais variaveis presentes nos aquivos (por enquanto so tem 2 DEWP e TEMP)
# [] Filtro de intervalo de datas na tabela a ser filtrada
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
# df.show()

# Funcao para saber o nome do tipo da informacao escolhido
def tipoInformacao(numero):
  if(numero  == "1"):
  	return "DEWP"
  elif(numero  == "2"):
  	return "TEMP"
  else:
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
	print("1 \t DEWP")
	print("2 \t TEMP")

	tipoDaInfor01 = scanner(sys_in).nextLine()

	if(metodoCalculo == "1"):
		nomeTipo = tipoInformacao(tipoDaInfor01)
		resultado = df.groupBy().sum(nomeTipo).collect()[0][0] / df.count()
		print("\n\nO resultado da media para o atributo", nomeTipo, "foi: \n", resultado, "\n")

	elif(metodoCalculo == "2"):
		nomeTipo = tipoInformacao(tipoDaInfor01)
		media = df.groupBy().sum(nomeTipo).collect()[0][0] / df.count()

		# Campo do tipo que sera utilizado
		variavelTipo = "df." + nomeTipo

		tabelaFill = df.select(df.DATE, eval(variavelTipo), ((eval(variavelTipo) - media)*(eval(variavelTipo) - media)).alias('aux'))
		resultado = (tabelaFill.groupBy().sum("aux").collect()[0][0]/ (df.count()-1)) ** 0.5
		print("\n\nO resultado do desvio padrao para o atributo", nomeTipo, "foi: \n", resultado, "\n")

	elif(metodoCalculo == "3"):
		print("Digite o segundo tipo de informacao")
		tipoDaInfor02 = scanner(sys_in).nextLine()
		nomeTipo1 = tipoInformacao(tipoDaInfor01)
		nomeTipo2 = tipoInformacao(tipoDaInfor02)

		mediax = df.groupBy().sum(nomeTipo1).collect()[0][0] / df.count() 
		mediay = df.groupBy().sum(nomeTipo2).collect()[0][0] / df.count() 

		variavelTipo1 = "df." + nomeTipo1
		variavelTipo2 = "df." + nomeTipo2

		tabelaCalc = df.select(eval(variavelTipo1), (eval(variavelTipo1)*(eval(variavelTipo2)-mediay)).alias('aux = xi(yi-mediay)'), eval(variavelTipo2), (eval(variavelTipo1)*(eval(variavelTipo1)-mediax)).alias('aux = xi(xi-mediax)'))
		b = tabelaCalc.groupBy().sum("aux = xi(yi-mediay)").collect()[0][0] / tabelaCalc.groupBy().sum("aux = xi(xi-mediax)").collect()[0][0]
		a = mediay - b * mediax


		print("\n\nO valor da variavel b calculada é: ",b)
		print("\n\nO valor da variavel a calculada é: ",a)


	else:
		print("Codigo não encontrado, por favor tente novamente!")
		continue
