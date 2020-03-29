# E-commerce user tagging system
__*Supports real-life business scenarios to assist precision marketing and advertising strategy based on the five major dimensions (user information, marketing reminders, consumption levels, product preferences, and efficiency improvements).*__

## Technical Framework
* Use __*Sqoop*__ to synchronize data from __*MySQL*__ to __*Hive*__
* Use __*Spark*__ to complete data cleaning and ETL tasks and refines a high-dimensional wide table for user data.
* Use __*ElasticSearch*__ and __*Kibana*__ to complete the relationship mapping of data query and user labels.
* The backend is based on __*SpringBoot*__, the frontend is based on __*Vue.js*__ and use __*Echart.js*__ for data display.

## Basic Features
* __Dashboard and data download for multi-condition queries__
![](https://res.cloudinary.com/dvrxfispp/image/upload/v1585461859/Github/spark-es-tag/tags_o4uz2f.png)

* __Data visualization for user portrait__
![](https://res.cloudinary.com/dvrxfispp/image/upload/v1585461858/Github/spark-es-tag/data1_coqxws.png)
![](https://res.cloudinary.com/dvrxfispp/image/upload/v1585461859/Github/spark-es-tag/data2_oc81fl.png)
![](https://res.cloudinary.com/dvrxfispp/image/upload/v1585461858/Github/spark-es-tag/data3_iyvwbt.png)
