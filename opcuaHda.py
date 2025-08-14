import asyncua.ua as ua
import asyncio
import pymongo
import os
import uuid
import traceback

from typing import List, Tuple, Union
from asyncua.server.history import HistoryStorageInterface
from asyncua.ua import NodeId, DataValue, Variant, VariantType, LocalizedText
from asyncua.server import Server
from asyncua.common.events import Event
from datetime import timedelta, datetime, timezone
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
    def _nodeid_to_bson(nid: ua.NodeId) -> dict:
        return {"ns": nid.NamespaceIndex, "id": nid.Identifier, "t": int(nid.NodeIdType)}

    @staticmethod
    def _nodeid_from_bson(d: dict) -> ua.NodeId:
        return ua.NodeId(d["id"], d["ns"], ua.NodeIdType(d["t"]))

    @staticmethod
    def datavalue_to_dict(datavalue: DataValue):
        return {
            'variant': datavalue.Value.VariantType.value if datavalue.Value else VariantType.Null.value,
            'timestamp': datavalue.SourceTimestamp if isinstance(datavalue.SourceTimestamp, datetime) else datetime.now(timezone.utc),
            'value': datavalue.Value.Value if datavalue.Value else None,
            'server_timestamp': datavalue.ServerTimestamp if isinstance(datavalue.ServerTimestamp, datetime) else datetime.now(timezone.utc)
        }
    
    @staticmethod
    def datavalue_from_dict(data: dict) -> DataValue:
        try:
            variant = Variant(
                Value=data.get('value', 0.0), 
                VariantType=VariantType(data.get('variant', VariantType.Double.value))
            )
            
            timestamp = data.get('timestamp')
            if not isinstance(timestamp, datetime) or timestamp.tzinfo is None:
                timestamp = datetime.now(timezone.utc)
                
            server_timestamp = data.get('server_timestamp')
            if not isinstance(server_timestamp, datetime) or server_timestamp.tzinfo is None:
                server_timestamp = datetime.now(timezone.utc)
            
            return DataValue(variant, SourceTimestamp=timestamp, ServerTimestamp=server_timestamp)
        
        except Exception as e:
            print(f"Erro ao converter dict para DataValue: {e}")
            variant = Variant(Value=0.0, VariantType=VariantType.Double)
            return DataValue(variant, SourceTimestamp=datetime.now(timezone.utc), ServerTimestamp=datetime.now(timezone.utc))
        
    @staticmethod
    def event_to_dict(event: Event) -> dict:
        def _vt(vt): 
            return int(ua.VariantType(vt))

        out = {}
        try:
            v = getattr(event, "EventId", None)
            val = getattr(v, "Value", None)
            out["EventId"] = {"VariantType": _vt(ua.VariantType.ByteString),
                            "Value": val if val is not None else uuid.uuid4().bytes}
        except Exception:
            out["EventId"] = {"VariantType": _vt(ua.VariantType.ByteString),
                            "Value": uuid.uuid4().bytes}
        try:
            v = getattr(event, "EventType", None)
            nid = getattr(v, "Value", None)
            if nid is not None:
                out["EventType"] = {"VariantType": _vt(ua.VariantType.NodeId),
                                    "Value": HistoryMongoDB._nodeid_to_bson(nid)}
        except Exception:
            pass
        try:
            v = getattr(event, "SourceNode", None)
            nid = getattr(v, "Value", None)
            if nid is not None:
                out["SourceNode"] = {"VariantType": _vt(ua.VariantType.NodeId),
                                    "Value": HistoryMongoDB._nodeid_to_bson(nid)}
        except Exception:
            pass
        try:
            v = getattr(event, "SourceName", None)
            val = getattr(v, "Value", None)
            if val is not None:
                out["SourceName"] = {"VariantType": _vt(ua.VariantType.String), "Value": val}
        except Exception:
            pass
        for field in ("Time", "ReceiveTime"):
            try:
                v = getattr(event, field, None)
                val = getattr(v, "Value", None)
                if not isinstance(val, datetime):
                    val = datetime.now(timezone.utc)
                out[field] = {"VariantType": _vt(ua.VariantType.DateTime), "Value": val}
            except Exception:
                out[field] = {"VariantType": _vt(ua.VariantType.DateTime),
                            "Value": datetime.now(timezone.utc)}
        try:
            v = getattr(event, "Message", None)
            txt = ""
            if v is not None:
                lv = getattr(v, "Value", None)
                txt = getattr(lv, "Text", None) or str(lv) or ""
            out["Message"] = {"VariantType": _vt(ua.VariantType.LocalizedText), "Value": txt}
        except Exception:
            out["Message"] = {"VariantType": _vt(ua.VariantType.LocalizedText), "Value": ""}

        try:
            v = getattr(event, "Severity", None)
            val = getattr(v, "Value", None)
            if val is None:
                val = 100
            out["Severity"] = {"VariantType": _vt(ua.VariantType.UInt16), "Value": val}
        except Exception:
            out["Severity"] = {"VariantType": _vt(ua.VariantType.UInt16), "Value": 100}

        return out
    
    @staticmethod
    def event_from_dict(data: dict) -> Event:
        ev = Event()

        def _vt(v):
            return ua.VariantType(v) if isinstance(v, int) else ua.VariantType(int(v))

        for k, meta in data.items():
            if k == "_id":
                continue

            vt  = meta.get("VariantType")
            val = meta.get("Value")

            if k == "Message":
                lt = LocalizedText(Text=(val or ""))
                ev.add_property(k, Variant(lt, ua.VariantType.LocalizedText), None)

            elif k in ("EventType", "SourceNode"):
                if isinstance(val, dict):
                    nid = ua.NodeId(val.get("id"), val.get("ns"), ua.NodeIdType(val.get("t")))
                    ev.add_property(k, Variant(nid, ua.VariantType.NodeId), None)

            elif k in ("Time", "ReceiveTime"):
                if not isinstance(val, datetime):
                    try:
                        val = datetime.fromisoformat(val)
                    except Exception:
                        val = datetime.now(timezone.utc)
                if val.tzinfo is None:
                    val = val.replace(tzinfo=timezone.utc)
                ev.add_property(k, Variant(val, ua.VariantType.DateTime), None)

            elif k == "EventId":
                if isinstance(val, str):
                    try:
                        val = bytes.fromhex(val)
                    except Exception:
                        val = uuid.uuid4().bytes
                ev.add_property(k, Variant(val, ua.VariantType.ByteString), None)

            elif k == "Severity":
                try:
                    sval = 100 if val is None else int(val)
                except Exception:
                    sval = 100
                ev.add_property(k, Variant(sval, ua.VariantType.UInt16), None)

            else:
                try:
                    ev.add_property(k, Variant(val, _vt(vt)), None)
                except Exception:
                    ev.add_property(k, Variant(str(val), ua.VariantType.String), None)

        return ev

    
    async def init(self):
        db = self.connection[self.database_name]
        await db['events'].create_index('Time.Value', name='events_time_idx')

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
    
    async def new_historized_event(self, source_id, evtypes, period, count=0):
        print(f"new_historized_event: {source_id}, {evtypes}, {period}, {count}")

    async def save_event(self, event: Event):
        try:
        
            if (getattr(getattr(event, "Message", None), "Value", None) is None):
                event.Message = ua.Variant(ua.LocalizedText(""), ua.VariantType.LocalizedText)
            if (getattr(getattr(event, "Severity", None), "Value", None) is None):
                event.Severity = ua.Variant(100, ua.VariantType.UInt16)

            msg_lt = getattr(getattr(event, "Message", None), "Value", None)
            msg_txt = getattr(msg_lt, "Text", "") if msg_lt else ""
            sev_val = getattr(getattr(event, "Severity", None), "Value", 100) or 100
            if msg_txt == "" and sev_val == 100:
                return
            
            event.ReceiveTime = ua.Variant(datetime.now(timezone.utc), ua.VariantType.DateTime)
            
            event_dict = self.event_to_dict(event)
            print("EVENT_DICT:", event_dict)  
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
                        
                        datavalue = ua.DataValue(
                            Value=ua.Variant(current_value, ua.VariantType.Double),
                                SourceTimestamp=datetime.now(timezone.utc),
                                ServerTimestamp=datetime.now(timezone.utc)
                        )
                        
                        await history_manager.save_node_value(node.nodeid, datavalue)
                        
                    except Exception as e:
                        print(f"Erro salvando {var_name}: {e}")
            
        except Exception as e:
            print(f"Erro salvamento forçado: {e}")
            await asyncio.sleep(5)