<h1 align="center">
  <img src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/6.x/svgs/solid/shield-halved.svg" alt="SentinelX Logo" width="120" height="120"/>
  <br>
  SentinelX
</h1>

<p align="center">
  <b>Un SIEM (Security Information and Event Management) Ligero, Escalable y Contenerizado.</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi" alt="FastAPI">
  <img src="https://img.shields.io/badge/Astro-FF5D01?style=for-the-badge&logo=astro&logoColor=white" alt="Astro">
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker">
  <img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL">
</p>

<p align="center">
  <a href="#características">Características</a> •
  <a href="#arquitectura">Arquitectura</a> •
  <a href="#quick-start-instalación-zero-touch">Quick Start</a> •
  <a href="#screenshots">Capturas</a> •
  <a href="README.md">🇬🇧 Read in English</a>
</p>

---

## 🛡️ Descripción

**SentinelX** es una plataforma SIEM de alto rendimiento diseñada para ingestar, parsear, normalizar y correlacionar logs de seguridad en toda tu infraestructura. Impulsada por *workers* asíncronos altamente escalables, enriquece los eventos mediante datos GeoIP y aplica reglas de detección de anomalías en tiempo real.

Ya sea que gestiones un VPS individual o un entorno distribuido de microservicios, SentinelX te otorga una visibilidad y correlación profunda con **una instalación de un solo comando**.

## ✨ Características

- **🚀 Despliegue Automatizado (Zero-Touch)**: SentinelX maneja de forma inteligente la generación de contraseñas, tu archivo `.env`, las redes de Docker y los proxys inversos (Nginx) usando un auto-instalador interactivo.
- **⚡ Procesamiento Asíncrono**: Arquitectura desacoplada con `parsing_workers` y `engine_workers` comunicándose con PostgreSQL. Escalable horizontalmente desde Docker.
- **🌍 Enriquecimiento de Entidades y GeoIP**: Mapeo automático de IPs a ASN, países y dominios para encontrar anomalías en milisegundos.
- **📜 Normalización Multi-Servicio**: Parsers nativos para `Apache`, `Nginx`, `Exim`, `Dovecot`, `SSH`, y `ModSecurity`.
- **🎯 Motor Dinámico de Reglas**: Evaluador de anomalías con sistemas de puntuación por comportamiento y decaimiento temporal.
- **🖥️ Interfaz Ultra Rápida**: Un Dashboard asombroso y ágil construido en Astro y JavaScript moderno.

---

## 📸 Tour Visual y Capturas de Pantalla

<p align="center">
  <i>Explora la interfaz de SentinelX: Moderna, receptiva y diseñada para una visibilidad profunda de seguridad.</i>
</p>

| **1. Panel Ejecutivo** | **2. Alertas Correlacionadas** |
|:---:|:---:|
| <img src="https://github.com/user-attachments/assets/7aa93035-bccb-434e-a4a1-ba8302b6d8fb" alt="Resumen del Dashboard" width="100%"> | <img src="https://github.com/user-attachments/assets/7fd10a8e-ad74-4cc9-a823-9485edc13247" alt="Dashboard de Alertas" width="100%"> |
| *Gráficos de actividad en tiempo real y KPIs de seguridad.* | *Vista centralizada de amenazas detectadas y correlaciones.* |

| **3. Detalle Forense Profundo** | **4. Gestión de Incidentes** |
|:---:|:---:|
| <img src="https://github.com/user-attachments/assets/b0407ba1-253d-4c9e-9e34-568c34905acf" alt="Detalles de Alerta" width="100%"> | <img src="https://github.com/user-attachments/assets/ac772408-84cf-4aa0-8cd1-a908526d3caf" alt="Investigación de Incidentes" width="100%"> |
| *Recolección de evidencia, incluyendo logs crudos y métricas.* | *Gestión completa del ciclo de vida para amenazas activas.* |

| **5. Inteligencia de Entidades** | **6. Procesos del Motor** |
|:---:|:---:|
| <img src="https://github.com/user-attachments/assets/a6c7ce6a-59cb-4944-87fa-d97957304e1e" alt="Puntuación de Riesgo de Entidad" width="100%"> | <img src="https://github.com/user-attachments/assets/eda9bd2b-e0ec-43c7-95f7-97b1a675cf53" alt="Procesos del Sistema" width="100%"> |
| *Análisis de comportamiento y puntuación de riesgo para IPs.* | *Monitoreo de la salud del motor y tuberías de ingesta.* |

---

## 🏗️ Arquitectura

SentinelX utiliza un diseño desacoplado Productor/Consumidor sumamente eficiente gracias a la contenerización nativa con Docker.

```mermaid
flowchart TD
    A[Servidores/Nodos\nSentinelX Agent] -->|Sube Logs| B(FastAPI Backend)
    B -->|Guarda Datos Crudos| DB[(PostgreSQL)]
    
    PW[Parsing Workers\nAuto-Escalables] -->|Extraen Crudos| DB
    PW -->|Normalizan y Enriquecen\nGeoIP / ASN| DB
    
    EW[Engine Workers\nCorrelacionadores] -->|Detectan Patrones\nScoring y Decaimiento| DB
    EW -->|Generan| C{Alertas e Incidentes}
    
    UI[Astro Frontend] <-->|Rest API| B
```

---

## ⚡ Quick Start (Instalación Zero-Touch)

Desplegar un SIEM complejo nunca había sido tan fácil. Proveemos un script de instalación "one-click" a medida que auto-genera contraseñas seguras, ajusta las variables de entorno, soluciona mitigaciones de Red (como las de CSF) y construye y sirve la interfaz frontend sin que tengas que tocar código.

**Requisitos Previos**:
- Linux (Ubuntu/Debian/RHEL/Alma) recomendado.
- `Docker` y `Docker Compose (v2)`.

### Instalación en 1 Comando

```bash
git clone https://github.com/yourusername/SentinelX-Neubox.git
cd SentinelX-Neubox

# Arrancar el orquestador
bash setup_sentinelx.sh
```

**¿Qué hace el script por ti?**
1. Te preguntará si estás en un ambiente `Local` (Install Rápida) o un entorno `Servidor` (Dominio Público).
2. Auto-generará contraseñas criptográficamente seguras para tu `POSTGRES_PASSWORD`, `SECRET_KEY`, y tu `INITIAL_ADMIN_PASSWORD`.
3. Validará la base de datos `GeoLite2` o se la saltará en modo rápido.
4. Levantará contenedores Nginx aislados localmente, o autoconfigurará tu Nginx global para tu dominio.
5. Escalará tus asynchronus-workers mágicamente (`docker compose up --scale parsing_worker=2`).

### Acceso a la Plataforma
En cuanto el script termine, imprime tus crendenciales Auto-Generadas en la terminal. ¡Cópialas!
- **Modo Local:** `http://localhost:4321`
- **Modo Servidor:** `https://tu-dominio-configurado.com`

---

## 📈 Escalamiento de Workers

SentinelX permite aumentar su poder de procesamiento sobre la marcha modificando la capa en Docker Compose:

```bash
# Agregar más parsing workers para ingestas masivas de logs
docker compose up -d --scale parsing_worker=4 --scale engine_worker=2
```

---

## 🤝 Contribuciones

¡Cualquier mejora es bienvenida! 

1. Haz un Fork del Proyecto
2. Crea tu rama de mejora (`git checkout -b feature/NuevaReglaDeDeteccion`)
3. Haz un Commit de tus cambios (`git commit -m 'Agrega reglas para SSH escalabilidad'`)
4. Haz Push a tu rama (`git push origin feature/NuevaReglaDeDeteccion`)
5. Abre un Pull Request

---

## 📄 Licencia
Este proyecto es código abierto. Revisa el archivo [LICENSE](LICENSE) para más detalles.
