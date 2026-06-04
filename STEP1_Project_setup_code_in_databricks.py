
# project setup creation and code development

#step1: create catalog

  CREATE CATALOG company 
  
#step2: create database 

  create database company.retaildatabase;
  
#step3: create volume

  create volume company.retaildatabase.retailvolume;
#step4: inside volume we created three layers meladian architecture

      ### bronze layer for raw data storage
      ### silver layer for claen data storage
      ### gold layer for efficient data storage
