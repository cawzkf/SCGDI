# SCGDI OPC UA – Monitoramento de Motor Trifásico (50cv)

## Objetivo
Implementar um servidor **OPC UA** em Python com:
- Nodeset customizado para um motor de 50cv  
- Integração via **MQTT** (subscribe em tópicos dos sensores)  
- **Alarmes e Eventos** (com severidades conforme regras do enunciado)  
- **Histórico** de variáveis elétricas e de eventos em MongoDB  
- **Discovery** automático em LDS (Local Discovery Server)  

---

## Arquitetura
```
Sensores → Broker MQTT (lse.dev.br) → Servidor OPC UA (asyncua) → UaExpert
                                     ↘ MongoDB (histórico de variáveis e eventos)
```

- Broker MQTT: `lse.dev.br`  
- Banco de dados: MongoDB Atlas  
- SDK OPC UA: [asyncua](https://github.com/FreeOpcUa/opcua-asyncio)  
- Cliente MQTT: gmqtt  

---

## Estrutura do Projeto
```
SCGDI/
├─ opcuaServer.py        # Servidor OPC UA, nodeset, alarmes/eventos, discovery
├─ opcuaHda.py           # Persistência de histórico em MongoDB
├─ mqttt.py              # Cliente MQTT (conexão, publish/subscribe)
├─ .gitignore
└─ README.md
```

---

## Configuração

## Instalação
```bash
git clone https://github.com/cawzkf/SCGDI.git
pip install -r requirements.txt


### Conexão MQTT
- Host: `lse.dev.br`  
- Porta: `1883`  
- Tópicos assinados:  
  - `scgdi/motor/electrical`  
  - `scgdi/motor/environment`  
  - `scgdi/motor/vibration`  

---

## Estrutura do Nodeset
```
Motor50CV
├── Electrical
│   ├── VoltageA / B / C
│   ├── CurrentA / B / C
│   ├── PowerActive / Reactive / Apparent
│   ├── EnergyActive / Reactive / Apparent
│   ├── PowerFactor
│   └── Frequency
├── Environment
│   ├── Temperature
│   ├── Humidity
│   └── CaseTemperature
└── Vibration
    ├── Axial
    └── Radial
```

---

## Regras de Alarmes e Eventos
- Overvoltage: tensão > 10% nominal (220 V ±10%) – Severidade 700  
- Undervoltage: tensão < 10% nominal – Severidade 700  
- Overcurrent: corrente > 10% nominal (10.5 A ±10%) – Severidade 700  
- CriticalTemperature: temperatura da carcaça > 60°C – Severidade 900  

Eventos são gravados no MongoDB e podem ser lidos no UaExpert (aba *Events*).

---

## Histórico
- Histórico de variáveis habilitado em:  
  - Tensões, Correntes, Potências, Energias, Frequência, Fator de Potência  
  - Temperatura, Umidade e CaseTemperature  
  - Vibração Axial e Radial  
- Histórico de eventos gravado em coleção `events` no MongoDB  

---

## Instalação
```bash
git clone https://github.com/cawzkf/SCGDI.git
cd SCGDI
```

---

## Execução
```bash
python opcuaServer.py
```

O servidor:
- Conecta ao broker MQTT  
- Registra-se no LDS  
- Cria o nodeset `Motor50CV`  
- Atualiza variáveis a partir do MQTT  
- Gera alarmes/eventos e salva histórico no MongoDB  

---

## Testes
- **UaExpert**: conectar pelo Discovery, navegar em `Objects/Motor50CV`, visualizar variáveis e eventos.  
- **MQTT Explorer**: publicar JSONs nos tópicos para simular sensores.  
- **MongoDB**: verificar coleções criadas automaticamente com histórico de variáveis e eventos.  
