# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit
import pandas as pd
from pyspark.sql.functions import desc
import math
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import mean


# COMMAND ----------

spark = SparkSession.builder.appName("Spark Basic Operations").getOrCreate()
spark

# COMMAND ----------

storage_account_name = 'finalassignment2'
storage_account_access_key = 'Z0AmOBh0l9WcV9f8WO8z1aKL9U7p+N9xrH/yKRPuJAS6Ub9eEvqcfNpwDSeM2Z4qcxvkoUXTMt3rPSzuUEOQXg=='
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)


# COMMAND ----------

blob_container = 'finalassignment3'
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/Open_Data_RDW__Gekentekende_voertuigen.csv"
df1 = spark.read.format("csv").load(filePath, inferSchema = True, header = True)

# COMMAND ----------

display(df1)

# COMMAND ----------

blob_container = 'finalassignment3'
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/Open_Data_RDW__Gekentekende_voertuigen_brandstof.csv"
df2 = spark.read.format("csv").load(filePath, inferSchema = True, header = True)

# COMMAND ----------

blob_container = 'finalassignment3'
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/Open_Data_RDW__Geconstateerde_Gebreken.csv"
df3 = spark.read.format("csv").load(filePath, inferSchema = True, header = True)

# COMMAND ----------

blob_container = 'finalassignment3'
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/Gebreken.csv"
df4 = spark.read.format("csv").load(filePath, inferSchema = True, header = True)

# COMMAND ----------

blob_container = 'finalassignment3'
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/Open_Data_Parkeren__PARKEERADRES (1).csv"
df5 = spark.read.format("csv").load(filePath, inferSchema = True, header = True)

# COMMAND ----------

blob_container = 'finalassignment3'
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/Open_Data_Parkeren__GPS-CO_RDINATEN_PARKEERLOCATIE (1).csv"
df6 = spark.read.format("csv").load(filePath, inferSchema = True, header = True)

# COMMAND ----------

display(df2)

# COMMAND ----------

df2.describe()

# COMMAND ----------

display(df3)

# COMMAND ----------

display(df4)

# COMMAND ----------

display(df5)

# COMMAND ----------

display(df6)

# COMMAND ----------

df1.select("Merk").show(10)

# COMMAND ----------

df1=df1.drop("Maximale constructiesnelheid (brom/snorfiets)", "Laadvermogen", "Oplegger geremd", "Aanhangwagen autonoom geremd", "Aanhangwagen middenas geremd", "Aantal staanplaatsen", "Afwijkende maximum snelheid", "Europese voertuigcategorie toevoeging", "Europese uitvoeringcategorie toevoeging", "Vervaldatum tachograaf", "Maximum ondersteunende snelheid", "Vervaldatum tachograaf DT", "Maximum last onder de vooras(sen) (tezamen)/koppeling", "Type remsysteem voertuig code", "Rupsonderstelconfiguratiecode", "Wielbasis voertuig minimum", "Wielbasis voertuig maximum", "Lengte voertuig minimum", "Lengte voertuig maximum", "Breedte voertuig minimum", "Breedte voertuig maximum", "Hoogte voertuig", "Hoogte voertuig minimum", "Hoogte voertuig maximum", "Massa bedrijfsklaar minimaal", "Massa bedrijfsklaar maximaal", "Technisch toelaatbaar massa koppelpunt", "Maximum massa technisch maximaal", "Maximum massa technisch minimaal", "Subcategorie Nederland", "Verticale belasting koppelpunt getrokken voertuig", "Type gasinstallatie" )

# COMMAND ----------

display(df1)

# COMMAND ----------

df2=df2.drop("Brandstofverbruik buiten de stad", "Brandstofverbruik stad", "CO2 uitstoot gecombineerd", "CO2 uitstoot gewogen", "Geluidsniveau rijdend", "Geluidsniveau stationair", "Emissieklasse", "Milieuklasse EG Goedkeuring (licht)", "Milieuklasse EG Goedkeuring (zwaar)", "Uitstoot deeltjes (licht)", "Uitstoot deeltjes (zwaar)", "Nettomaximumvermogen", "Nominaal continu maximumvermogen", "Roetuitstoot", "Toerental geluidsniveau", "Emissie deeltjes type1 wltp", "Emissie co2 gecombineerd wltp", "Emissie co2 gewogen gecombineerd wltp", "Brandstof verbruik gecombineerd wltp", "Brandstof verbruik gewogen gecombineerd wltp", "Elektrisch verbruik enkel elektrisch wltp", "Actie radius enkel elektrisch wltp", "Actie radius enkel elektrisch stad wltp", "Elektrisch verbruik extern opladen wltp", "Actie radius extern opladen wltp", "Actie radius extern opladen stad wltp", "Max vermogen 15 minuten", "Max vermogen 60 minuten", "Netto max vermogen elektrisch", "Klasse hybride elektrisch voertuig", "Opgegeven maximum snelheid", "Uitlaatemissieniveau")

# COMMAND ----------

display(df2)

# COMMAND ----------

df5=df5.drop("TelephoneNumber", "EmailAddress", "FaxNumber")

# COMMAND ----------

display(df5)

# COMMAND ----------

df6=df6.drop("EndDateLocation")

# COMMAND ----------

display(df6)

# COMMAND ----------

groupbymerk=df1.groupBy("Merk").count().display(1000)

# COMMAND ----------

groupbyeerstekleur=df1.groupBy("Eerste kleur").count().display(1000)

# COMMAND ----------

groupbycilinderinhoud=df1.groupBy("Cilinderinhoud").count().display(1000)

# COMMAND ----------

groupbygasmeterial=df2.groupBy("Brandstof omschrijving").count().display(1000)

# COMMAND ----------

groupbyfuelconsumption=df2.groupBy("Brandstofverbruik gecombineerd").count().display(1000)

# COMMAND ----------

groupbyparkinglocation=df5.groupBy("Place").count().display(1000)

# COMMAND ----------

df1.describe()

# COMMAND ----------

df1=df1.withColumnRenamed("Kenteken", "Kenteken2")

# COMMAND ----------


df1.join(df2,df1.Kenteken2 ==  df2.Kenteken,"left") \
     .display(truncate=False)

# COMMAND ----------

dfcars=df1.join(df2,df1["Kenteken2"] ==  df2["Kenteken"],"left")

# COMMAND ----------

dfcars.display()

# COMMAND ----------

dfdamage=df3.join(df4,df3["Gebrek identificatie"] ==  df4["Gebrek identificatie"],"left")

# COMMAND ----------

dfdamage.display()

# COMMAND ----------

dfparking=df5.join(df6,df5["ParkingAddressReferenceType"] ==  df6["LocationReferenceType"],"left")

# COMMAND ----------

dfparking.display()

# COMMAND ----------

dffullcars=dfcars.join(dfdamage,dfcars["Kenteken2"] ==  dfdamage["Kenteken"],"left")

# COMMAND ----------

dffullcars.display()

# COMMAND ----------

print("Distinct count: "+str(dffullcars.count()))

# COMMAND ----------

dffullcars1=dffullcars.dropDuplicates(["Kenteken2"])

# COMMAND ----------

dffullcars1.display()

# COMMAND ----------

