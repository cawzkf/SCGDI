import asyncua.ua as ua
import asyncio
import pymongo
import os
import uuid

from typing import List, Tuple, Union
from asyncua.server.history import HistoryStorageInterface
from asyncua.ua import NodeId, DataValue, Variant, VariantType, LocalizedText
from asyncua.server import Server
from asyncua.common.events import Event
from datetime import timedelta, datetime
from asyncua.ua import HistoryReadResult, HistoryData

from dotenv import load_dotenv

load_dotenv()

class HistoryMongoDB(HistoryStorageInterface):
    def __init__(self, server: Server, max_history_data_response_size=10000):
        mongo_uri = os.getenv('MONGO_URI')
        self.connection = pymongo.AsyncMongoClient(mongo_uri)
        
        self.database_name = "opcua-simulation-sensors"
        self.server = server
        super().__init__(max_history_data_response_size)

    async def get_parent_name(self, node_id: NodeId) -> str:
        node = self.server.get_node(node_id)
        node_name = (await node.read_browse_name()).Name

        parent = await node.get_parent()
        parent_name = (await parent.read_browse_name()).Name

        return f'{parent_name}_{node_name}'

    @staticmethod
    def datavalue_to_dict(datavalue: DataValue):
        return {
            'variant': datavalue.Value.VariantType.value if datavalue.Value else VariantType.Null.value,
            'timestamp': datavalue.SourceTimestamp if isinstance(datavalue.SourceTimestamp, datetime) else datetime.now(),
            'value': datavalue.Value.Value if datavalue.Value else None,
            'server_timestamp': datavalue.ServerTimestamp if isinstance(datavalue.ServerTimestamp, datetime) else datetime.now()
        }
    
    @staticmethod
    def datavalue_from_dict(data: dict) -> DataValue:
        try:
            variant = Variant(
                Value=data.get('value', 0.0), 
                VariantType=VariantType(data.get('variant', VariantType.Double.value))
            )
            
            timestamp = data.get('timestamp')
            if not isinstance(timestamp, datetime):
                timestamp = datetime.now()
                
            server_timestamp = data.get('server_timestamp')
            if not isinstance(server_timestamp, datetime):
                server_timestamp = datetime.now()
            
            return DataValue(variant, SourceTimestamp=timestamp, ServerTimestamp=server_timestamp)
        
        except Exception as e:
            print(f"Erro ao converter dict para DataValue: {e}")
            variant = Variant(Value=0.0, VariantType=VariantType.Double)
            return DataValue(variant, SourceTimestamp=datetime.now(), ServerTimestamp=datetime.now())
        
    @staticmethod
    def event_to_dict(event: Event) -> dict:
        fields = {}
        try:
            try:
                if hasattr(event, 'EventId') and event.EventId is not None:
                    if hasattr(event.EventId, 'Value') and event.EventId.Value is not None:
                        fields['EventId'] = {"VariantType": VariantType.ByteString, "Value": event.EventId.Value}
                    else:
                        fields['EventId'] = {"VariantType": VariantType.ByteString, "Value": uuid.uuid4().bytes}
                else:
                    fields['EventId'] = {"VariantType": VariantType.ByteString, "Value": uuid.uuid4().bytes}
            except:
                fields['EventId'] = {"VariantType": VariantType.ByteString, "Value": uuid.uuid4().bytes}
            
            try:
                if hasattr(event, 'Time') and event.Time is not None:
                    if hasattr(event.Time, 'Value') and event.Time.Value is not None:
                        fields['Time'] = {"VariantType": VariantType.DateTime, "Value": event.Time.Value}
                    else:
                        fields['Time'] = {"VariantType": VariantType.DateTime, "Value": datetime.now()}
                else:
                    fields['Time'] = {"VariantType": VariantType.DateTime, "Value": datetime.now()}
            except:
                fields['Time'] = {"VariantType": VariantType.DateTime, "Value": datetime.now()}
            
            try:
                if hasattr(event, 'ReceiveTime') and event.ReceiveTime is not None:
                    if hasattr(event.ReceiveTime, 'Value') and event.ReceiveTime.Value is not None:
                        fields['ReceiveTime'] = {"VariantType": VariantType.DateTime, "Value": event.ReceiveTime.Value}
                    else:
                        fields['ReceiveTime'] = {"VariantType": VariantType.DateTime, "Value": datetime.now()}
                else:
                    fields['ReceiveTime'] = {"VariantType": VariantType.DateTime, "Value": datetime.now()}
            except:
                fields['ReceiveTime'] = {"VariantType": VariantType.DateTime, "Value": datetime.now()}
            
            try:
                if hasattr(event, 'Message') and event.Message is not None:
                    if hasattr(event.Message, 'Value') and event.Message.Value is not None:
                        if hasattr(event.Message.Value, 'Text') and event.Message.Value.Text is not None:
                            fields['Message'] = {"VariantType": VariantType.LocalizedText, "Value": event.Message.Value.Text}
                        else:
                            fields['Message'] = {"VariantType": VariantType.LocalizedText, "Value": "No text content"}
                    else:
                        fields['Message'] = {"VariantType": VariantType.LocalizedText, "Value": "No message value"}
                else:
                    fields['Message'] = {"VariantType": VariantType.LocalizedText, "Value": "No message attribute"}
            except:
                fields['Message'] = {"VariantType": VariantType.LocalizedText, "Value": "Message processing error"}
            
            try:
                if hasattr(event, 'Severity') and event.Severity is not None:
                    if hasattr(event.Severity, 'Value') and event.Severity.Value is not None:
                        fields['Severity'] = {"VariantType": VariantType.UInt16, "Value": event.Severity.Value}
                    else:
                        fields['Severity'] = {"VariantType": VariantType.UInt16, "Value": 100}
                else:
                    fields['Severity'] = {"VariantType": VariantType.UInt16, "Value": 100}
            except:
                fields['Severity'] = {"VariantType": VariantType.UInt16, "Value": 100}
                
        except Exception as e:
            print(f"Erro em event_to_dict: {e}")
            fields = {
                "EventId": {"VariantType": VariantType.ByteString, "Value": uuid.uuid4().bytes},
                "Time": {"VariantType": VariantType.DateTime, "Value": datetime.now()},
                "ReceiveTime": {"VariantType": VariantType.DateTime, "Value": datetime.now()},
                "Message": {"VariantType": VariantType.LocalizedText, "Value": "Error processing event"},
                "Severity": {"VariantType": VariantType.UInt16, "Value": 100}
            }
        
        return fields

    @staticmethod  
    def event_from_dict(data: dict) -> Event:
        event = Event()
        for field, value in data.items():
            if field == '_id':
                continue

            if field == "Message":
                event.add_property(
                    name=field,
                    val=Variant(
                        Value=LocalizedText(Text=value["Value"]),
                        VariantType=VariantType(value["VariantType"])
                    ),
                    datatype=None
                )
                continue

            event.add_property(
                name=field,
                val=Variant(
                    Value=value["Value"],
                    VariantType=VariantType(value["VariantType"])
                ),
                datatype=None
            )

        return event

    async def test_connection(self):
        try:
            await self.connection.admin.command('ping')
            print(f"MongoDB conectado: {self.database_name}")
        
            db = self.connection[self.database_name]
            collections = await db.list_collection_names()
            if collections:
                print(f"Colletions encontradas: {collections}")
            else:
                print("Nenhuma coleção ainda, criando automaticamente")
            
            return True
        except Exception as e:
            print(f"Erro MongoDB: {e}")
            return False

    async def init(self):
        await self.test_connection()

    async def stop(self):
        if self.connection:
            await self.connection.close()

    async def historize_data_change(self, node, period, count=0):
        try:
            node_id = node.nodeid
            await self.new_historized_node(node_id, period, count)
            print(f"Histórico habilitado para: {node_id}")
        except Exception as e:
            print(f"Erro ao habilitar histórico para {node.nodeid}: {e}")
 
    async def historize_event(self, source, period, count=0):
        try:
            source_id = source.nodeid
            await self.new_historized_event(source_id, None, period, count)
            print(f"Histórico de eventos habilitado para: {source_id}")
        except Exception as e:
            print(f"Erro ao habilitar histórico de eventos para {source.nodeid}: {e}")

    async def new_historized_node(self, node_id: NodeId, period: timedelta, count=0):
        try:
            name_parente = await self.get_parent_name(node_id)
            db = self.connection[self.database_name]
            
            await db[name_parente].create_index(
                'server_timestamp', 
                unique=False,  
                name='server_timestamp_index'
            )
                
        except Exception as e:
            print(f"Erro em new_historized_node: {e}")

    async def save_node_value(self, node_id: NodeId, datavalue: DataValue):
        try:
            collection_name = await self.get_parent_name(node_id)
            db = self.connection[self.database_name]
            
            document = self.datavalue_to_dict(datavalue)
            document['forced_save'] = True
            
            await db[collection_name].insert_one(document)
            print(f"SALVO: {collection_name} = {datavalue.Value.Value if datavalue.Value else 'None'}")
            
        except Exception as e:
            print(f"ERRO save_node_value: {e}")

    async def read_node_history(self, node_id: NodeId, start: datetime, end: datetime, nb_values: int) -> Tuple[List[DataValue], Union[datetime, None]]:
        try:
            db = self.connection[self.database_name]
            collection_name = await self.get_parent_name(node_id)
            
            query = {
                "server_timestamp": {
                    "$gte": start,
                    "$lte": end
                }
            }

            result = []
            count = await db[collection_name].count_documents(query)
            
            if count == 0:
                return [], None
            
            cursor = db[collection_name].find(query).sort('server_timestamp', pymongo.ASCENDING).limit(nb_values)
        
            async for document in cursor:
                try:
                    datavalue = self.datavalue_from_dict(document)
                    result.append(datavalue)
                except Exception as e:
                    print(f"Erro ao converter documento: {e}")
                    continue

            continuation = result[-1].ServerTimestamp if count > nb_values and result else None
            return result, continuation
        
        except Exception as e:
            print(f"ERRO read_node_history: {e}")
            return [], None

    async def read_history(self, params):
        try:
            results = []
            
            for node_to_read in params.NodesToRead:
                result = HistoryReadResult()
                
                try:
                    start_time = datetime.now() - timedelta(hours=2)  
                    end_time = datetime.now()
                    max_values = 100 
                    
                    if hasattr(params, 'ReadDetails') and params.ReadDetails:
                        details = params.ReadDetails
                        if hasattr(details, 'StartTime') and details.StartTime:
                            start_time = details.StartTime
                        if hasattr(details, 'EndTime') and details.EndTime:
                            end_time = details.EndTime
                        if hasattr(details, 'NumValuesPerNode') and details.NumValuesPerNode:
                            max_values = min(details.NumValuesPerNode, 50)
                    
                    node_id = node_to_read.NodeId
                    data_values, continuation = await self.read_node_history(
                        node_id, start_time, end_time, max_values
                    )

                    if not data_values:
                        history_data = HistoryData()
                        history_data.DataValues = []
                    else:
                        history_data = HistoryData()
                        history_data.DataValues = data_values
                    
                    result.StatusCode = ua.StatusCodes.Good
                    result.HistoryData = history_data
                    result.ContinuationPoint = None
    
                    
                except Exception as e:
                    print(f"Erro ao processar no {node_to_read.NodeId}: {e}")
                    import traceback
                    traceback.print_exc()
                    
                    result.StatusCode = ua.StatusCodes.BadNoData
                    result.HistoryData = HistoryData()
                    result.HistoryData.DataValues = []
                    result.ContinuationPoint = None
                
                results.append(result)
            
            return results
            
        except Exception as e:
            print(f"Erro crítico em read_history: {e}")
            import traceback
            traceback.print_exc()
            
            results = []
            for _ in params.NodesToRead:
                result = HistoryReadResult()
                result.StatusCode = ua.StatusCodes.BadInternalError
                result.HistoryData = HistoryData()
                result.HistoryData.DataValues = []
                results.append(result)
            return results
    
    async def new_historized_event(self, source_id, evtypes, period, count=0):
        print(f"new_historized_event: {source_id}, {evtypes}, {period}, {count}")

    async def save_event(self, event: Event):
        try:
            event_dict = self.event_to_dict(event)
            db = self.connection[self.database_name]
            await db['events'].insert_one(event_dict)
        except Exception as e:
            print(f"ERRO save_event: {e}")

    async def read_event_history(
            self, source_id: NodeId, start: datetime, end: datetime, nb_events: int, select_clauses) -> Tuple[List[Event], Union[datetime, None]]:
        try:
            db = self.connection[self.database_name]
            query = {"$and": [
                {"Time.Value": {"$gte": start}},
                {"Time.Value": {"$lte": end}}
            ]}

            count = await db['events'].count_documents(query)
            cursor = db['events'].find(query).sort('Time.Value', pymongo.ASCENDING).limit(nb_events)
            events = []

            async for document in cursor:
                try:
                    events.append(self.event_from_dict(document))
                except Exception as e:
                    print(f"Erro ao converter evento: {e}")

            continuation = events[-1].Time.Value if count > nb_events and events else None
            return events, continuation
        
        except Exception as e:
            print(f"ERRO read_event_history: {e}")
            return [], None

async def force_save_all_periodically(server, history_manager, nodes_variables):
    while True:
        try:
            await asyncio.sleep(10)
            
            vars_to_save = [
                "VoltageA", "VoltageB", "VoltageC",
                "CurrentA", "CurrentB", "CurrentC", 
                "Temperature", "Humidity", "CaseTemperature",
                "EnergyActive", "EnergyReactive", "EnergyApparent", 
                "PowerFactor", "PowerApparent",
                "PowerActive", "PowerReactive", "Frequency",
                "Axial", "Radial"
            ]
        
            for var_name in vars_to_save:
                if var_name in nodes_variables:
                    try:
                        node = nodes_variables[var_name]
                        current_value = await node.read_value()
                        
                        from asyncua import ua
                        datavalue = ua.DataValue(
                            Value=ua.Variant(current_value, ua.VariantType.Double),
                            SourceTimestamp=datetime.now(),
                            ServerTimestamp=datetime.now()
                        )
                        
                        await history_manager.save_node_value(node.nodeid, datavalue)
                        
                    except Exception as e:
                        print(f"Erro salvando {var_name}: {e}")
            
        except Exception as e:
            print(f"Erro salvamento forçado: {e}")
            await asyncio.sleep(5)