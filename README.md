# Apache Kafka Sample 🚀

Proyecto de demostración que implementa una arquitectura de **Apache Kafka** con servicios productores y consumidores en **C#/.NET**. La solución utiliza Docker Compose para orquestar los contenedores y GitHub Actions para automatizar el proceso de construcción, testing y publicación de imágenes Docker.

---

## 📋 Tabla de Contenidos

- [Descripción General](#descripción-general)
- [Arquitectura](#arquitectura)
- [Requisitos Previos](#requisitos-previos)
- [Configuración Docker Compose](#configuración-docker-compose)
- [Configuración GitHub Actions](#configuración-github-actions)
- [Instalación y Uso](#instalación-y-uso)
- [Estructura del Proyecto](#estructura-del-proyecto)

---

## 📖 Descripción General

Este proyecto implementa un sistema de **mensajería asincrónica** utilizando Apache Kafka con:

- **WebAPI**: Servicio productor que genera eventos de usuarios
- **Consumer**: Servicios consumidores que procesan los eventos
- **Kafka Cluster**: 3 brokers + 3 controllers en modo KRaft (sin ZooKeeper)
- **Automatización CI/CD**: GitHub Actions para compilación, testing y deployment

### Componentes Principales

| Componente | Descripción |
|-----------|------------|
| **Kafka Controllers** | 3 nodos de control (controller-1, controller-2, controller-3) |
| **Kafka Brokers** | 3 brokers (broker-1, broker-2, broker-3) en puertos 29092, 39092, 49092 |
| **WebAPI** | API REST productora en puerto 13000 |
| **Consumers** | 2 instancias consumidoras (consumer-1, consumer-2) |
| **Topic Init** | Job de inicialización que crea el tópico "users" |

---

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────┐
│           Docker Compose Network                     │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │Controller│  │Controller│  │Controller│          │
│  │   (1)    │  │   (2)    │  │   (3)    │          │
│  └──────────┘  └──────────┘  └──────────┘          │
│       ▲              ▲              ▲               │
│       └──────────────┼──────────────┘               │
│                      │                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │ Broker   │  │ Broker   │  │ Broker   │          │
│  │  (1)     │  │  (2)     │  │  (3)     │          │
│  │:29092    │  │:39092    │  │:49092    │          │
│  └──────────┘  └──────────┘  └──────────┘          │
│       ▲              ▲              ▲               │
│       └──────────────┼──────────────┘               │
│                      │                              │
│           ┌──────────▼──────────┐                   │
│           │   Topic: "users"    │                   │
│           │  (3 particiones)    │                   │
│           └─────────┬──────────┘                    │
│                     │                               │
│    ┌────────────────┼────────────────┐              │
│    │                │                │              │
│ ┌──▼──┐        ┌─────▼────┐    ┌────▼──┐          │
│ │WebAPI│        │ Consumer │    │Consumer│         │
│ │:13000│        │   (1)    │    │  (2)   │         │
│ └──────┘        └──────────┘    └────────┘         │
│                                                      │
└─────────────────────────────────────────────────────┘
```

---

## 🔧 Requisitos Previos

### Software Requerido

- **Docker**: v20.10 o superior
- **Docker Compose**: v1.29 o superior
- **.NET SDK**: 8.0, 9.0 o 10.0
- **Git**: Para clonar el repositorio

### Verificar Instalación

```bash
docker --version
docker-compose --version
dotnet --version
```

---

## 🐳 Configuración Docker Compose

### Archivo: `docker-compose.yml`

El proyecto incluye una configuración completa de Docker Compose que orquesta todos los servicios necesarios para la arquitectura Kafka.

#### **1. Nodos Controladores (KRaft)**

```yaml
controller-1:
  image: apache/kafka:latest
  container_name: controller-1
  environment:
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: controller
    KAFKA_LISTENERS: CONTROLLER://:9093
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@controller-1:9093,2@controller-2:9093,3@controller-3:9093
  networks:
    - network
```

**Características**:
- **Modo KRaft**: Elimina la dependencia de ZooKeeper
- **3 Controladores**: Alta disponibilidad del cluster
- **Red Interna**: Comunicación entre nodos en el puerto 9093

**Variables Principales**:
- `KAFKA_NODE_ID`: Identificador único de cada nodo (1, 2, 3)
- `KAFKA_PROCESS_ROLES: controller`: Define el nodo como controlador
- `KAFKA_CONTROLLER_QUORUM_VOTERS`: Lista de todos los controladores para el quórum

#### **2. Nodos Brokers (Productores/Consumidores)**

```yaml
broker-1:
  image: apache/kafka:latest
  container_name: broker-1
  ports:
    - 29092:9092  # Puerto expuesto al host
  environment:
    KAFKA_NODE_ID: 4
    KAFKA_PROCESS_ROLES: broker
    KAFKA_LISTENERS: 'PLAINTEXT://:19092,PLAINTEXT_HOST://:9092'
    KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://broker-1:19092,PLAINTEXT_HOST://localhost:29092'
    KAFKA_MIN_INSYNC_REPLICAS: 2
    KAFKA_DEFAULT_REPLICATION_FACTOR: 3
    KAFKA_NUM_PARTITIONS: 3
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: true
  depends_on:
    - controller-1
    - controller-2
    - controller-3
  healthcheck:
    test: [ "CMD", "/opt/kafka/bin/kafka-metadata-quorum.sh", "--bootstrap-server", "0.0.0.0:19092", "describe","--status" ]
    interval: 10s
    timeout: 5s
    retries: 5
  networks:
    - network
```

**Características**:
- **3 Brokers**: Réplicas para tolerancia a fallos
- **2 Listeners**: Comunicación interna y externa
- **Health Checks**: Validación del estado del broker
- **Replicación**: Factor de replicación 3, mínimo 2 replicas in-sync

**Puertos Expuestos**:
- `broker-1`: 29092 (localhost)
- `broker-2`: 39092 (localhost)
- `broker-3`: 49092 (localhost)

**Configuración de Replicación**:
- `KAFKA_DEFAULT_REPLICATION_FACTOR: 3`: Cada tópico se replica en 3 brokers
- `KAFKA_MIN_INSYNC_REPLICAS: 2`: Mínimo 2 replicas activas para confirmación
- `KAFKA_NUM_PARTITIONS: 3`: Tópicos con 3 particiones por defecto

#### **3. Inicializador de Tópicos**

```yaml
topic_init:
  image : apache/kafka:latest
  container_name: topic_init
  entrypoint: /opt/kafka/bin/kafka-topics.sh 
  command: > 
    --create --topic users --if-not-exists
    --bootstrap-server broker-1:19092
  depends_on:
    broker-1:
      condition: service_healthy
    broker-2:
      condition: service_healthy
    broker-3:
      condition: service_healthy
  networks:
    - network
```

**Características**:
- **Job Único**: Crea automáticamente el tópico "users"
- **Dependencias Salud**: Espera a que todos los brokers estén saludables
- **Idempotente**: `--if-not-exists` evita errores en re-ejecuciones

#### **4. WebAPI (Productor)**

```yaml
webapi:
  container_name: webapi
  image: edwardcoder423/webapi:v2
  ports:
    - 13000:13000
  environment:
    Producer__Bootstrap_servers: "broker-1:19092,broker-2:19092,broker-3:19092"
    Producer__Acks: "Leader"
    Producer__ClientId: 1
    ASPNETCORE_URLS: "http://0.0.0.0:13000"
  depends_on:
    broker-1:
      condition: service_healthy
    broker-2:
      condition: service_healthy
    broker-3:
      condition: service_healthy
    topic_init:
      condition: service_completed_successfully
  networks:
    - network
```

**Variables de Configuración**:
- `Producer__Bootstrap_servers`: Lista de brokers para conexión
- `Producer__Acks: "Leader"`: Confirmación del broker líder solamente
- `ASPNETCORE_URLS`: URL de escucha de la API (puerto 13000)

#### **5. Consumidores**

```yaml
consumer-1:
  container_name: consumer-1
  image: edwardcoder423/consumer:v2
  environment:
    Consumer__Bootstrap_Server: "broker-1:19092,broker-2:19092,broker-3:19092"
    Consumer__GroupId: "users-consumer"
    Consumer__ClientId: "consumer1"
  depends_on:
    broker-1:
      condition: service_healthy
    topic_init:
      condition: service_completed_successfully
  networks:
    - network
```

**Variables de Configuración**:
- `Consumer__Bootstrap_Server`: Brokers para conexión
- `Consumer__GroupId`: Grupo de consumidores (comparten el procesamiento)
- `Consumer__ClientId`: Identificador único del consumidor

#### **6. Red Compartida**

```yaml
networks:
  network:
    # Red tipo bridge personalizada para todos los servicios
```

**Ventajas**:
- Comunicación entre contenedores por nombre DNS
- Aislamiento de la red del host
- Escalabilidad horizontal

### Comandos Docker Compose

```bash
# Iniciar todos los servicios
docker-compose up -d

# Ver logs en tiempo real
docker-compose logs -f

# Ver logs de un servicio específico
docker-compose logs -f webapi

# Detener todos los servicios
docker-compose down

# Eliminar volúmenes asociados
docker-compose down -v

# Verificar estado de servicios
docker-compose ps
```

---

## ⚙️ Configuración GitHub Actions

El proyecto incluye dos workflows automatizados en `.github/workflows/`:

### **1. `build.yml` - Workflow Reutilizable**

```yaml
name: BuildNTest

on:
    workflow_call:

jobs:
  Build:
    strategy:
      matrix:
        os: [ubuntu-latest, windows-latest, macos-latest]
        dotnetv: [10.0.x, 9.0.x, 8.0.x]
        
    runs-on: ${{matrix.os}}
    steps:
      - name: CheckOut_Repo
        uses: actions/checkout@v4

      - name: SetUp_Dotnet
        uses: actions/setup-dotnet@v5
        with:
          dotnet-version: ${{matrix.dotnetv}}

      - name: Restore_Dep 
        run: dotnet restore

      - name: Build_Projects
        run: dotnet build

      - name: Run_Unit_Tests
        run: dotnet test
```

**Características**:
- **Workflow Reutilizable** (`workflow_call`): Puede ser llamado desde otros workflows
- **Matriz de Ejecución**: 9 combinaciones diferentes (3 SO × 3 versiones .NET)
- **Compatibilidad Multiplataforma**: Verifica en Ubuntu, Windows y macOS
- **Pasos Secuenciales**:
  1. Checkout del repositorio
  2. Instalación de .NET SDK
  3. Restauración de dependencias
  4. Compilación
  5. Ejecución de tests unitarios

### **2. `main_workflow.yml` - Workflow Principal**

```yaml
name: .NET_MainWorkFlow

on:
  workflow_dispatch:

jobs:
  Build_N_Test:
     uses: ./.github/workflows/build.yml
     
  Push_Images:
    runs-on: ubuntu-latest
    needs: Build_N_Test
    steps:
    - name: CheckOut_Repository
      uses: actions/checkout@v4
      
    - name: SetUp_Docker
      uses: docker/setup-docker-action@v5.1.0

    - name: Login_Docker
      uses: docker/login-action@v4.1.0
      with:
        username: ${{secrets.DOCKER_USERNAME}}
        password: ${{secrets.DOCKER_PSSD}}

    - name: BuildNPush_WebApi
      uses: docker/build-push-action@v7.1.0
      with:
        file: ./Dockerfile.Webapi
        push: true
        tags: edwardcoder423/webapi:latest

    - name: BuildNPush_Consumer
      uses: docker/build-push-action@v7.1.0
      with:
        file: ./Dockerfile.Consumer
        push: true
        tags: edwardcoder423/consumer:latest
```

**Características**:
- **Trigger Manual** (`workflow_dispatch`): Se ejecuta bajo demanda
- **Ejecución en Fases**:
  - **Fase 1**: Compilación y testing (reutiliza `build.yml`)
  - **Fase 2**: Build y push de imágenes Docker (ejecuta solo si Fase 1 es exitosa)
  
- **Dependencias**: `needs: Build_N_Test` asegura que los tests pasen antes del push

#### Pasos del Workflow Principal

| Paso | Descripción | Action |
|------|-------------|--------|
| **CheckOut** | Descargar código | `actions/checkout@v4` |
| **SetUp Docker** | Configurar Docker | `docker/setup-docker-action@v5.1.0` |
| **Login Docker** | Autenticarse en Docker Hub | `docker/login-action@v4.1.0` |
| **Build WebAPI** | Compilar y publicar imagen | `docker/build-push-action@v7.1.0` |
| **Build Consumer** | Compilar y publicar imagen | `docker/build-push-action@v7.1.0` |

#### Configuración de Secretos

Para que el workflow funcione correctamente, configura los siguientes secretos en GitHub:

1. Ve a **Settings** → **Secrets and variables** → **Actions**
2. Crea los siguientes secretos:

| Secreto | Valor |
|---------|-------|
| `DOCKER_USERNAME` | Tu usuario de Docker Hub |
| `DOCKER_PSSD` | Tu contraseña o token de Docker Hub |

```bash
# Verificar credenciales Docker
docker login -u tu_usuario
```

### Flujo de Ejecución

```
┌─────────────────────────────────────────────────────┐
│     GitHub Actions: .NET_MainWorkFlow               │
├─────────────────────────────────────────────────────┤
│                                                      │
│  1️⃣ Trigger Manual (workflow_dispatch)              │
│       ↓                                              │
│  2️⃣ Build_N_Test Job                                │
│       ├─ Reutiliza: build.yml                       │
│       ├─ Matriz: 9 combinaciones (SO × .NET)       │
│       └─ Resultado: ✅ PASS / ❌ FAIL               │
│       ↓                                              │
│  3️⃣ Push_Images Job (solo si 2️⃣ = ✅)               │
│       ├─ Setup Docker                               │
│       ├─ Login Docker Hub                           │
│       ���─ Build & Push WebAPI                        │
│       ├─ Build & Push Consumer                      │
│       └─ Resultado: Imágenes publicadas en Hub      │
│                                                      │
└─────────────────────────────────────────────────────┘
```

### Monitoreo de Workflows

```bash
# Ver workflows en repositorio
gh workflow list

# Ver ejecuciones de un workflow
gh run list --workflow=main_workflow.yml

# Ver detalles de una ejecución
gh run view <run-id>

# Ver logs de una ejecución
gh run view <run-id> --log
```

---

## 🚀 Instalación y Uso

### 1. Clonar el Repositorio

```bash
git clone https://github.com/Edward-Code007/Apache_Kafka_Sample.git
cd Apache_Kafka_Sample
```

### 2. Construir Localmente (Opcional)

```bash
# Restaurar dependencias
dotnet restore

# Compilar proyecto
dotnet build

# Ejecutar tests
dotnet test
```

### 3. Iniciar con Docker Compose

```bash
# Iniciar todos los servicios
docker-compose up -d

# Esperar a que todos los servicios estén saludables
# (approximately 30-60 seconds)

# Verificar estado
docker-compose ps
```

### 4. Verificar la Instalación

```bash
# Ver logs del WebAPI
docker-compose logs webapi

# Ver logs del Consumer
docker-compose logs consumer-1

# Verificar que el tópico fue creado
docker-compose exec broker-1 /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server localhost:19092
```

### 5. Usar la API

```bash
# Crear un evento de usuario
curl -X POST http://localhost:13000/api/users \
  -H "Content-Type: application/json" \
  -d '{"name": "John Doe", "email": "john@example.com"}'

# Ver los logs del consumidor procesando el evento
docker-compose logs -f consumer-1
```

### 6. Detener los Servicios

```bash
# Detener todos los servicios
docker-compose down

# Detener y eliminar volúmenes
docker-compose down -v
```

---

## 📁 Estructura del Proyecto

```
Apache_Kafka_Sample/
├── .github/
│   └── workflows/
│       ├── build.yml                 # Workflow reutilizable de compilación
│       └── main_workflow.yml         # Workflow principal de CI/CD
├── Kafka_First_Aproach.Webapi/      # Proyecto WebAPI (Productor)
│   ├── Controllers/
│   ├── Services/
│   └── Kafka_First_Aproach.Webapi.csproj
├── Kafka_First_Aproach.Consumer/    # Proyecto Consumidor
│   ├── Services/
│   └── Kafka_First_Aproach.Consumer.csproj
├── Kafka_First_Aproach.Contracts/   # Modelos compartidos
│   └── Kafka_First_Aproach.Contracts.csproj
├── Kafka_First_Aproach.Test/        # Pruebas unitarias
│   └── Kafka_First_Aproach.Test.csproj
├── Dockerfile.Webapi                 # Imagen Docker para WebAPI
├── Dockerfile.Consumer               # Imagen Docker para Consumer
├── docker-compose.yml                # Orquestación de contenedores
├─�� KAFKA_FIRST_APROACH.slnx         # Solución .NET
└── README.md                         # Este archivo
```

### Archivos Clave

| Archivo | Propósito |
|---------|-----------|
| `docker-compose.yml` | Configuración de 8 servicios: 3 controllers, 3 brokers, webapi, consumers, topic init |
| `Dockerfile.Webapi` | Multi-stage build para compilar y empaquetar WebAPI |
| `Dockerfile.Consumer` | Multi-stage build para compilar y empaquetar Consumer |
| `.github/workflows/build.yml` | Compilación y testing en múltiples plataformas |
| `.github/workflows/main_workflow.yml` | CI/CD completo: test → build → push |

---

## 🔍 Detalles Técnicos

### Configuración Kafka KRaft

El cluster utiliza el modo **KRaft** (Kafka Raft) que:
- ✅ Elimina la dependencia de ZooKeeper
- ✅ Simplifica la configuración
- ✅ Mejora el rendimiento
- ✅ Facilita la operación

### Multi-stage Docker Builds

Ambos Dockerfiles utilizan construcción multi-stage:

```dockerfile
# Stage 1: Build
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
COPY ./src ./src
RUN dotnet restore && dotnet publish -c Release -o /app

# Stage 2: Runtime
FROM mcr.microsoft.com/dotnet/aspnet:10.0
COPY --from=build /app .
ENTRYPOINT ["dotnet", "webapi.dll"]
```

**Ventajas**:
- 📉 Imágenes más pequeñas (solo runtime, sin SDK)
- 🔒 Mayor seguridad (menos herramientas innecesarias)
- ⚡ Deployments más rápidos

### Health Checks

Los brokers incluyen health checks que verifican:
- Estado del quórum KRaft
- Disponibilidad del broker
- Intervalos de chequeo: 10 segundos
- Timeout: 5 segundos
- Reintentos: 5 intentos

---

## 📚 Recursos Útiles

### Documentación Oficial

- [Apache Kafka](https://kafka.apache.org/documentation/)
- [Kafka KRaft Mode](https://kafka.apache.org/documentation/#kraft)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)

### Comandos Kafka Útiles

```bash
# Listar tópicos
docker-compose exec broker-1 /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server localhost:19092

# Describir tópico
docker-compose exec broker-1 /opt/kafka/bin/kafka-topics.sh \
  --describe --topic users --bootstrap-server localhost:19092

# Productor de prueba
docker-compose exec broker-1 /opt/kafka/bin/kafka-console-producer.sh \
  --topic users --bootstrap-server localhost:19092

# Consumidor de prueba
docker-compose exec broker-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --topic users --from-beginning --bootstrap-server localhost:19092
```

---

## 🤝 Contribuciones

Las contribuciones son bienvenidas. Por favor:

1. Fork el repositorio
2. Crea una rama para tu feature (`git checkout -b feature/nueva-feature`)
3. Commit tus cambios (`git commit -am 'Añade nueva feature'`)
4. Push a la rama (`git push origin feature/nueva-feature`)
5. Abre un Pull Request

---

## 📝 Licencia

Este proyecto está bajo licencia MIT. Ver el archivo LICENSE para más detalles.

---

## 👤 Autor

**Edward-Code007**

- GitHub: [@Edward-Code007](https://github.com/Edward-Code007)
- Docker Hub: [@edwardcoder423](https://hub.docker.com/u/edwardcoder423)

---

## 📞 Soporte

Si encuentras problemas:

1. Verifica los logs: `docker-compose logs -f`
2. Asegúrate de que Docker y Docker Compose están instalados
3. Revisa la sección de [Requisitos Previos](#requisitos-previos)
4. Abre un issue en el repositorio

---

**Última actualización**: Mayo 2026
**Versión**: 2.0
