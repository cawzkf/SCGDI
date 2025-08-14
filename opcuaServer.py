import asyncio
import logging
import json
import os
from datetime import timedelta, datetime, timezone

from mqttt import MQTTChannel
from opcuaHda import HistoryMongoDB, force_save_all_periodically
from asyncua import Server, Client
from asyncua.ua import ObjectIds, VariantType, Variant, LocalizedText
import asyncua.ua as ua
from dotenv import load_dotenv
import uuid
import traceback

def callback_mqtt_message_handler(nos, client, topic, payload: bytes, qos, properties):
    try:
        data = json.loads(payload.decode('utf-8'))
        mapa = {   
            ('voltage', 'a'): 'VoltageA',
            ('voltage', 'b'): 'VoltageB', 
            ('voltage', 'c'): 'VoltageC',
            ('current', 'a'): 'CurrentA',
            ('current', 'b'): 'CurrentB',
            ('current', 'c'): 'CurrentC',
            ('power', 'active'): 'PowerActive',
            ('power', 'reactive'): 'PowerReactive',
            ('power', 'apparent'): 'PowerApparent',
            ('energy', 'active'): 'EnergyActive',
            ('energy', 'reactive'): 'EnergyReactive',
            ('energy', 'apparent'): 'EnergyApparent',
            ('powerFactor',): 'PowerFactor',
            ('frequency',): 'Frequency',
            ('temperature',): 'Temperature',
            ('humidity',): 'Humidity',
            ('caseTemperature',): 'CaseTemperature',
            ('axial',): 'Axial',
            ('radial',): 'Radial'
        }
            
        for chaves, var_name in mapa.items():
            if var_name in nos:
                try:
                    if len(chaves) == 2:
                        if chaves[0] in data and chaves[1] in data[chaves[0]]:
                            value = data[chaves[0]][chaves[1]]
                            asyncio.create_task(nos[var_name].set_value(value))
                            print(f"{var_name} = {value}")
                    elif len(chaves) == 1:
                        if chaves[0] in data:
                            value = data[chaves[0]]
                            asyncio.create_task(nos[var_name].set_value(value))
                            print(f"{var_name} = {value}")                      
                except Exception as e:
                    print(f"Erro ao definir {var_name}: {e}")
                    
    except json.JSONDecodeError as e:
        print(f'Erro ao decodificar JSON: {e}')
    except Exception as e:
        print(f'Erro MQTT: {e}')

async def init_mqtt():
    canal_mqtt = MQTTChannel()
    await canal_mqtt.init()

    canal_mqtt.client.subscribe('scgdi/motor/electrical')
    canal_mqtt.client.subscribe('scgdi/motor/vibration')
    canal_mqtt.client.subscribe('scgdi/motor/environment')

    return canal_mqtt

async def task_register_discovery(server: Server, registration_interval = 5):
    lds_endpoint = os.getenv('LDS_ENDPOINT')
    if not lds_endpoint:
        print("LDS_ENDPOINT não configurado")
        return
    while True:
        try:
            async with Client(lds_endpoint) as client:
                await client.register_server(server)
                print("Servidor registrado no LDS")
        except Exception as e:
            print(f"Erro no registro de discovery: {e}")
        finally:
            await asyncio.sleep(registration_interval)

async def generate_event_properly(event_generator, event_type, message, severity, history_manager, **extra_props):
    try:
        event_generator.event.EventId = ua.Variant(uuid.uuid4().bytes, ua.VariantType.ByteString)
        event_generator.event.Time = ua.Variant(datetime.now(timezone.utc), ua.VariantType.DateTime)
        event_generator.event.ReceiveTime = ua.Variant(datetime.now(timezone.utc), ua.VariantType.DateTime)
        event_generator.event.Message = ua.Variant(ua.LocalizedText(message or ""), ua.VariantType.LocalizedText)
        event_generator.event.Severity = ua.Variant(severity, ua.VariantType.UInt16)

        for name, val in extra_props.items():
            if hasattr(event_generator.event, name):
                if isinstance(val, str):
                    v = ua.Variant(val, ua.VariantType.String)
                elif isinstance(val, bool):
                    v = ua.Variant(val, ua.VariantType.Boolean)
                elif isinstance(val, int):
                    v = ua.Variant(val, ua.VariantType.Int32)
                elif isinstance(val, float):
                    v = ua.Variant(val, ua.VariantType.Float)
                else:
                    v = ua.Variant(str(val), ua.VariantType.String)
                setattr(event_generator.event, name, v)

        # await history_manager.save_event(event_generator.event)
        await event_generator.trigger()

    except Exception as e:
        print(f"Erro ao gerar evento: {e}")
        traceback.print_exc()

async def check_voltage_events(nodes_variables, event_generator, history_manager):

    tensao_nominal = 220.0  
    tolerancia = 0.1  
    
    for fase in ['A', 'B', 'C']:
        try:
            variavel_tensao = f"Voltage{fase}"
            if variavel_tensao not in nodes_variables:
                continue
                
            tensao = await nodes_variables[variavel_tensao].get_value()
            
            if tensao > tensao_nominal * (1 + tolerancia):
                await generate_event_properly(
                    event_generator,
                    event_type="Overvoltage",
                    message="Overvoltage detected",
                    severity=700,
                    history_manager=history_manager,
                    VoltagePhase=fase
                )
            
            elif tensao < tensao_nominal * (1 - tolerancia):
                await generate_event_properly(
                    event_generator,
                    event_type="Undervoltage", 
                    message="Undervoltage detected",
                    severity=700,
                    history_manager=history_manager,
                    VoltagePhase=fase
                )
                
        except Exception as e:
            print(f"Erro ao verificar tensão fase {fase}: {e}")

async def check_current_events(nodes_variables, event_generator, history_manager):
    corrente_nominal = 10.5  
    tolerancia = 0.1  
    
    for fase in ['A', 'B', 'C']:
        try:
            variavel_corrente = f"Current{fase}"
            if variavel_corrente not in nodes_variables:
                continue
                
            corrente = await nodes_variables[variavel_corrente].get_value()
            
            if corrente > corrente_nominal * (1 + tolerancia):
                await generate_event_properly(
                    event_generator,
                    event_type="Overcurrent",
                    message="Overcurrent detected",  
                    severity=700,
                    history_manager=history_manager,
                    CurrentPhase=fase
                )
                
        except Exception as e:
            print(f"Erro ao verificar corrente fase {fase}: {e}")

async def check_temperature_events(nodes_variables, event_generator, history_manager):
    try:
        if "CaseTemperature" not in nodes_variables:
            return
            
        temp_carcaca = await nodes_variables["CaseTemperature"].get_value()
        
        if temp_carcaca > 60:
            await generate_event_properly(
                event_generator,
                event_type="CriticalTemperature",
                message="Case temperature critical",
                severity=900,
                history_manager=history_manager,
                CaseTemperature=temp_carcaca
            )
            
    except Exception as e:
        print(f"Erro ao verificar temperatura: {e}")

async def configure_server_info(server: Server):
    alt_host = os.getenv('OPCUA_SERVER_HOST')
    alt_port = os.getenv('OPCUA_SERVER_PORT')
    
    alt_endpoint = f"opc.tcp://{alt_host}:{alt_port}"
    print(f"endpoint: {alt_endpoint}")
    
    await server.set_build_info(
        product_uri=os.getenv('OPCUA_PRODUCT_URI'),
        manufacturer_name=os.getenv('OPCUA_MANUFACTURER'),
        product_name=os.getenv('OPCUA_PRODUCT_NAME'),
        software_version=os.getenv('OPCUA_SOFTWARE_VERSION'),
        build_number=os.getenv('OPCUA_BUILD_NUMBER'),
        build_date=datetime.now(timezone.utc)
    )

    app_uri = os.getenv('OPCUA_APPLICATION_URI')
    await server.set_application_uri(app_uri)

    server.name = os.getenv('OPCUA_SERVER_NAME')
    server.product_uri = os.getenv('OPCUA_SERVER_PRODUCT_URI')
    
    print(f"Servidor configurado: {server.name}")

async def main():
    load_dotenv()
    
    server = Server()
    await server.init()
    await configure_server_info(server)


    storage = HistoryMongoDB(server)
    await storage.init()
    server.iserver.history_manager.set_storage(storage)
    
    canal_mqtt = await init_mqtt()

    endpoint = os.getenv('OPCUA_ENDPOINT')
    server.set_endpoint(endpoint)

    idx = await server.register_namespace(os.getenv('OPCUA_PRODUCT_URI'))

    motor50cv = await server.nodes.objects.add_folder(idx, "Motor50CV")
    electrical = await motor50cv.add_object(idx, "Electrical")
    environment = await motor50cv.add_object(idx, "Environment") 
    vibration = await motor50cv.add_object(idx, "Vibration")

    electrical_variables = [
        "VoltageA", "VoltageB", "VoltageC",
        "CurrentA", "CurrentB", "CurrentC", 
        "PowerActive", "PowerReactive", "PowerApparent",
        "EnergyActive", "EnergyReactive", "EnergyApparent",
        "PowerFactor", "Frequency"
    ]

    environmental_variables = ["Temperature", "Humidity", "CaseTemperature"]
    vibration_variables = ["Axial", "Radial"]

    nodes_variables = {}
   
    smc = {
        "Electrical": electrical_variables,
        "Environment": environmental_variables,
        "Vibration": vibration_variables
    }

    for condicao, variaveis in smc.items():
        if condicao == "Electrical":
            cont = electrical
        elif condicao == "Environment":
            cont = environment
        elif condicao == "Vibration":
            cont = vibration
    
        for variavel in variaveis:  
            no_val = await cont.add_variable(idx, variavel, 0.0)
            await no_val.set_writable()
            nodes_variables[variavel] = no_val

    canal_mqtt.client.on_message = lambda *args: callback_mqtt_message_handler(nodes_variables, *args)

    await server.start()

    variables_for_history = list(nodes_variables.keys())

    for nome_variavel in variables_for_history:
        try:
            node = nodes_variables[nome_variavel]

            await server.iserver.enable_history_data_change(
                node, 
                period=timedelta(seconds=5),  
                count=1000  
            )
            try:
                
                await node.write_attribute(
                    ua.AttributeIds.Historizing, 
                    ua.DataValue(ua.Variant(True, ua.VariantType.Boolean))
                )
                print(f"Histórico habilitado para: {nome_variavel}")
            except AttributeError:
                await server.set_attribute_value(
                    node.nodeid,
                    ua.AttributeIds.Historizing,
                    ua.DataValue(ua.Variant(True, ua.VariantType.Boolean))
                )

            
        except Exception as e:
            print(f"Erro ao configurar histórico para {nome_variavel}: {e}")

    try:
        etype = await server.create_custom_event_type(
            idx,
            "Motor50CVMonitoringEvent",
            ObjectIds.BaseEventType,
            [
                ("CaseTemperature", VariantType.Float),
                ("VoltagePhase", VariantType.String),
                ("CurrentPhase", VariantType.String),
            ],
        )

        event_generator = await server.get_event_generator(etype, motor50cv)
        try:
            await motor50cv.write_attribute(
                ua.AttributeIds.EventNotifier,
                ua.DataValue(ua.Variant(1, ua.VariantType.Byte))  
            )
        except AttributeError:
            await server.set_attribute_value(
                motor50cv.nodeid,
                ua.AttributeIds.EventNotifier,
                ua.DataValue(ua.Variant(1, ua.VariantType.Byte))
            )

        
        await server.iserver.enable_history_event(motor50cv, period=timedelta(seconds=1))
        
    except Exception as e:
        print(f"Erro ao configurar eventos: {e}")
        event_generator = None


    asyncio.create_task(force_save_all_periodically(server, storage, nodes_variables))
    asyncio.create_task(task_register_discovery(server, registration_interval=10))

    count = 0
    try:
        while True:
            if count >= 5 and event_generator:  
                await check_temperature_events(nodes_variables, event_generator, storage)
                await check_voltage_events(nodes_variables, event_generator, storage)
                await check_current_events(nodes_variables, event_generator, storage)
                count = 0
            else:
                count += 1
            
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\nParando servidor")
    finally:
        await storage.stop()
        await server.stop()
        print("Servidor parado")

if __name__ == "__main__":
    asyncio.run(main())