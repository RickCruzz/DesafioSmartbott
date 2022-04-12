#Resolução das Questões Levantadas.

#%%
from sqlalchemy import create_engine
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, regexp_replace
spark = SparkSession.builder.appName("analise").getOrCreate()


#%%
#1º Questão
#Lendo Dados
##Escreva uma query que retorna a quantidade de linhas na tabela Sales.SalesOrderDetail
##pelo campo SalesOrderID, desde que tenham pelo menos três linhas de detalhes.
df_pyspark = spark.read.csv('../data/Sales_SalesOrderDetail.csv', sep=';', header=True, inferSchema=True, nullValue='NULL', timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS')
df_pyspark.show()

#%%
#Agrupando Dados
#Cache/Count
df1 = df_pyspark.groupBy("SalesOrderID").count().filter("count >= 3")
df1.cache()
df1.count()
#FIM 1º QUESTAO

#%%
#2º Questão
##Escreva uma query que ligue as tabelas Sales_SalesOrderDetail, Sales_SpecialOfferProduct
##e Production_Product e retorne os 3 produtos (Name) mais vendidos (pela soma de
##OrderQty), agrupados pelo número de dias para manufatura (DaysToManufacture).

#df_details.createOrReplaceTempView("details")
#df1 = spark.sql("select count(*) from details")
#df1.show()

#Criar View Das Tabelas
df_pyspark = spark.read.csv('../data/Sales_SalesOrderDetail.csv', sep=';', header=True, inferSchema=True, nullValue='NULL', timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS')
df_pyspark.createOrReplaceTempView("sal_order_detail")

df_pyspark = spark.read.csv('../data/Sales_SpecialOfferProduct.csv', sep=';', header=True, inferSchema=True, nullValue='NULL', timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS')
df_pyspark.createOrReplaceTempView("sal_offer_product")

df_pyspark = spark.read.csv('../data/Production_Product.csv', sep=';', header=True, inferSchema=True, nullValue='NULL', timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS')
df_pyspark.createOrReplaceTempView("sal_prod_product")

#%%
df2 = spark.sql("""with cte_dedup_prods as (select prod.Name, prod.DaysToManufacture, sum(detail.OrderQty) as OrderTotal 
                from sal_order_detail as detail 
                inner join sal_offer_product as offer 
                on((detail.SpecialOfferID = offer.SpecialOfferID) and (detail.ProductID = offer.ProductID))
                inner join sal_prod_product as prod
                on(offer.ProductID = prod.ProductID)
                group By prod.Name, prod.DaysToManufacture
                Order by 
                3 desc)
                select Name, DaysToManufacture, OrderTotal,
                row_number() over(partition by DaysToManufacture order by OrderTotal desc) as rn
                from cte_dedup_prods""")
df2 = df2.filter(df2['rn'] <= 3).drop('rn')
df2.show()

# FIM 2º QUESTÃO

#%%
#3º Questão
#Escreva uma query ligando as tabelas Person_Person, Sales_Customer e
#Sales_SalesOrderHeader de forma a obter uma lista de nomes de clientes e uma contagem
#de pedidos efetuados.



#%%
#Geração de Views
df_pyspark = spark.read.csv('../data/Person_Person.csv', sep=';', header=True, inferSchema=True, nullValue='NULL', timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS')
#CriaColunaFullName
df_pyspark = df_pyspark.withColumn('FullName', concat_ws(' ', col('FirstName'), col('MiddleName') ,col('LastName')))
df_pyspark.createOrReplaceTempView("sal_person")

df_pyspark = spark.read.csv('../data/Sales_Customer.csv', sep=';', header=True, inferSchema=True, nullValue='NULL', timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS')
df_pyspark.createOrReplaceTempView("sal_customer")

df_pyspark = spark.read.csv('../data/Sales_SalesOrderHeader.csv', sep=';', header=True, inferSchema=True, nullValue='NULL', timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS')
df_pyspark.createOrReplaceTempView("sal_order_header")


#%%
df3 = spark.sql("""
                select 
                    person.FullName,
                    count(header.SalesOrderID) as OrderCount
                from 
                    sal_person as person
                    left join
                        sal_customer as cust
                            on(cust.PersonID = person.BusinessEntityID)
                    left join
                        sal_order_header as header
                            on (cust.CustomerID = header.CustomerID)
                group by 
                    person.FullName
                order by
                     2 desc""")
df3.show()
#df3.cache()
#df3.count()
##FIM 3º QUESTÃO


#%%
#4º Questão
#Escreva uma query usando as tabelas Sales.SalesOrderHeader, Sales.SalesOrderDetail e
#Production.Product, de forma a obter a soma total de produtos (OrderQty) por ProductID e
#OrderDate.
#sum(OrderQty) as OrderQty, productID e Order Date


df4 = spark.sql(
"""
select 
    sum(detail.OrderQty) as TotalProd,
    prod.ProductID, 
    header.OrderDate
from sal_order_header as header
    inner join sal_order_detail as detail
        on (header.SalesOrderID = detail.SalesOrderID)
    inner join sal_offer_product as offer 
        on((detail.SpecialOfferID = offer.SpecialOfferID) and (detail.ProductID = offer.ProductID))
    inner join sal_prod_product as prod
        on(offer.ProductID = prod.ProductID)
    group by prod.ProductID, header.OrderDate
    order by 3 desc
""")
df4.show()

### FIM 4º QUESTÃO

#%%
#5º Questão
#Escreva uma query mostrando os campos SalesOrderID, OrderDate e TotalDue da tabela
#Sales.SalesOrderHeader. Obtenha apenas as linhas onde a ordem tenha sido feita durante
#o mês de setembro/2011 e o total devido esteja acima de 1.000. Ordene pelo total devido
#decrescente.
#Solução 1

df5 = spark.sql ("select SalesOrderID, OrderDate, TotalDue from  sal_order_header where OrderDate between '2011-09-01' and '2011-09-30'")
df5 = df5.withColumn('TotalDue', regexp_replace('TotalDue', ',', '.').cast('decimal(14,4)'))
df5 = df5.filter(df5['TotalDue'] > 1000).orderBy(df5['TotalDue'].desc(), df5['OrderDate'].desc())
df5.show()


#%%
#Solução 2
df5 = spark.sql(""" with cte_dedup_totals as(
    select 
        SalesOrderID, OrderDate, 
    CAST(Replace(TotalDue,',','.') as Decimal(10,4)) as TotalDue 
        from  sal_order_header )
select * From cte_dedup_totals
    where 
    OrderDate between '2011-09-01' and '2011-09-30'
    and TotalDue > 1000
    Order by TotalDue desc, OrderDate desc
""")
df5.show()


#%%
#Exportar Postgres p/ Depois Metabase
#5432 - root/root
#Criando Sessão p/ Conexão

engine = create_engine('postgresql://root:root@localhost:5432/test_db')

#%%
#df1.show()
pandasDF = df1.toPandas()
pandasDF.to_csv('../output/1_questao_v_details.csv')
pandasDF.to_sql('v_details', engine)

#%%
#df2.show()
pandasDF = df2.toPandas()
pandasDF.to_csv('../output/2_questao_v_prod_days.csv')
pandasDF.to_sql('v_prod_days', engine)
#%%
#df3.show()
pandasDF = df3.toPandas()
pandasDF.to_csv('../output/3_questao_v_order_customers.csv')
pandasDF.to_sql('v_order_customers', engine)

#%%
#df4.show()
pandasDF = df4.toPandas()
pandasDF.to_csv('../output/4_questao_v_prod_orderdate.csv')
#pandasDF.to_sql('v_prod_orderdate', engine)

#%%
df5.show()
pandasDF = df5.toPandas()
pandasDF.to_csv('../output/5_questao_v_orders_set2011.csv')
pandasDF.to_sql('v_orders_set2011', engine)
