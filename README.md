# IoT Monitoring


This project is an AWS serverless backend deployment of a Iot monitoring project. The Iot was reporting the current volume of remote water tanks that were being used to power eletrical generators. The incomeing data streamed to elastic search (not included) where it was then picked up by the dataStaging script. Immeditly it was reduced from 1 point per second to 1 point per minute to improved performance and reduce data storage fees. From there the consumptionGET script removed outliers, identified recharges, discharges, and stable states, and modeled the average rate for each state. 

To best understand the logic behind the system see the notebooks in both English and Spanish.


Este proyecto es una implementación backend serverless de AWS de un proyecto para monitoreo Iots. La Iot estaba informandonos del volumen actual de unos estanques de agua remotos que utilizaron para alimentar generadores eléctricos. Los datos de ingresos se transfirieron a Elastic Search (no incluida) donde el script dataStaging lo recogió. Inmediatamente se redujo de 1 punto por segundo a 1 punto por minuto para mejorar el rendimiento y reducir las tarifas de almacenamiento de datos. A partir de ahí, el script de consumptionGET eliminó los valores atípicos, identificó las recargas, las descargas y los estados estables, y modeló la tasa promedio para cada estado.

Para comprender mejor la lógica detrás del sistema, vea los Notebooks en inglés y en español.
