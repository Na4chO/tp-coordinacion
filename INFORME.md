# Informe

## Coordinación de las instancias de Sum

Todas las instancias de Sum consumen de la misma cola de entrada (`INPUT_QUEUE`), donde el Gateway es el productor. Al ser consumidores que compiten por una única cola, los pares (fruta, cantidad) de cada cliente se reparten entre las distintas instancias, y cada una acumula una parte de los datos aplicando la función `Sum` de `FruitItem`.

El mensaje de fin (EOF) le llaga a una única instancia de Sum, y le comunica la cantidad total (`Total`) de mensajes enviados por el Cliente con `ClientId`.
La instancia de Sum que recibe el EOF se convierte en el **coordinador** del cierre de ese cliente con resto de las instancias **participantes**. Esta coordinación se da mediante un exchange, con tres tipos de mensajes:

1. **Coordinator (broadcast)**: Cuando un nodo recibe el EOF, publica un mensaje `Coordinator` con routing key de broadcast (`coord_broadcast`), en el que comunica su ID como CoordinatorId. Todas las demás instancias pasan a ser **participantes** del cliente.
2. **Count (participant-to-coordinator)**: cada participante le responde al coordinador (con `coord_<CoordinatorId>` como routing key) la cantidad de mensajes que procesó (`Processed`). Esto ocurre al recibir `Coordinator` y por cada lote de datos que el participante consume posterior a la llegada del mensaje `Coordinator`.
3. **End (broadcast)**: el coordinador resta los `Count` recibidos hasta confirmar que se procesaron todos los mensajes del cliente. Al completarse, publica `End` por broadcast y los participantes envían sus registros acumulados y su propio EOF hacia Aggregation.

De esta manera, solo el coordinador necesita conocer el total del cliente; los participantes reportan cuánto aportaron y el fin de la ingesta queda determinado por conteo de mensajes.

Al finalizar la ingesta, cada Sum particiona sus registros hacia Aggregation con el hash `clientFruitGroup(clientId, fruta) % AggregationAmount`. Así, para un mismo cliente, cada fruta cae siempre en el mismo Aggregation, quedando cada instancia de Aggregation a cargo de un subconjunto de frutas.

## Coordinación de las instancias de Aggregation

Cada instancia de Aggregation se bindea únicamente a su routing key `aggregation_<id>` del exchange de agregación, por lo que recibe solo las frutas asignadas a su grupo, provenientes de todos los Sum. Une los `FruitItem` finales para cada fruta con la función `Sum`.

Cada Sum envía un EOF por cliente a todos los Aggregation (broadcast del EOF). Por lo que cada Aggregation considera terminada la ingesta de un cliente cuando recibió `SumAmount` mensajes de EOF (uno por cada instancia de Sum). En ese momento calcula un **top parcial** (los `TopSize` mayores valores de su grupo, usando la función `Less` de `FruitItem`) y lo envía al Join por la cola de salida.

El Join espera un top parcial de cada una de las `AggregationAmount` instancias y los mergea comparando con `Less`, manteniendo el top final de tamaño `TopSize`. Ese top final se envía al Gateway, que lo entrega al cliente correcto mediante su `ClientId`.

## Escalabilidad

### Respecto a los clientes

- El Gateway asigna a cada cliente un identificador único (`MessageHandler.id`), presente en los mensajes pertenecientes a ese cliente.
- Los actores intermedios (Sum, Aggregation y Join) indexan estados para cada cliente por `ClientId`.
- La cola compartida de entrada permite que el trabajo de todos los clientes se reparta dinámicamente entre las instancias de Sum.

### Respecto a grandes volúmenes de datos

- **Sum**: al competir todas las instancias por la misma cola, aumentar la cantidad de Sum aumenta el paralelismo de procesamiento de los `FruitItem`
- **Aggregation**: el particionado por hash con key `(ClientId, Fruit)` permite que cada fruta de cada cliente sea procesada en un solo Aggregation. Aumentar `AGGREGATION_AMOUNT` distribuye las frutas en más grupos y reduce la carga por instancia. Si una gran cantidad clientes envían registros de la misma fruta, la carga también se reparte (evitando la sobrecarga de un unico Aggregation), ya que se tiene en cuenta `ClientId` al momento de hashear.
- **Reducción de tráfico**: los Sum envían a cada Aggregation solo los registros de su grupo (en lugar de hacer broadcast), y cada Aggregation envía al Join un único top parcial de tamaño `TopSize` en lugar de todos sus datos.

### Respecto a la cantidad de controles

- **SumAmount**: el protocolo de coordinación está parametrizado por la cantidad de instancias de Sum, ya que cada Aggregation espera una cantidad `SumAmount` de EOF. No depende de un número fijo de instancias.
- **AggregationAmount**: el hash de particionado acepta cualquier cantidad de grupos, y el Join espera `AggregationAmount` tops parciales antes de producir el top final.

Por lo tanto, cambiar la multiplicidad de los controles en el archivo de docker-compose no requiere modificar el código de los procesos.