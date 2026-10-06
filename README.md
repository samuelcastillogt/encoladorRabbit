# rabbit-enqueuer

Herramienta local para encolar mensajes desde un archivo JSON en una cola de RabbitMQ (por ejemplo los mensajes `CA` exportados de `PRD-dispatch-sap`).

## Uso

```bash
npm install
cp .env.example .env   # y completar RABBITMQ_URL
npm start              # http://localhost:3090
```

1. **Archivo JSON**: arrastra el archivo (un arreglo de mensajes). Puedes filtrar por `ESTADO` y ordenar por `FECHA_REGISTRO`.
2. **Cola destino**: lista las colas desde la API de management (puerto 15672); si no está disponible usa `PRESET_QUEUES`. También acepta un nombre manual. Se valida que la cola exista antes de publicar.
3. **Cantidad**: define **Desde el #** y **Cantidad**. Las filas que se van a encolar quedan resaltadas en la tabla.
4. **Encolar**: pide confirmación (avisa si el ambiente es PRD). Al terminar, **Desde** avanza al siguiente pendiente, así puedes encolar por tandas (10, 50, 100…).

Cada mensaje se publica en el default exchange (routing key = nombre de la cola), `persistent`, con header `x-trace-id` nuevo y `x-enqueued-by: rabbit-enqueuer`. Al final puedes descargar el resultado (ID, cola y traceId de cada mensaje).

### Opciones avanzadas

- **Reiniciar `retryCount`**: pone `messageConfig.retryCount = 0`. Sin esto, el consumer manda los mensajes con reintentos agotados a la DLX al primer fallo.
- **`lastExecution` = ahora**: ojo, `consumer-sap-dispatch` reencola sin procesar si `lastExecution` tiene menos de `INTERVALO_REINTENTOS_MILISEGUNDOS` (60 s por defecto).
- **Pausa entre mensajes / mensajes por request**: sirven para no saturar al consumer.
