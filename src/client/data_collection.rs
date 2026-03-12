use std::{fs::File, io::Write};

use chrono::Utc;
use csv::Writer;
use omnipaxos_kv::common::{
    kv::{ClientId, CommandId, NodeId},
    utils::Timestamp,
};
use serde::Serialize;

use crate::configs::ClientConfig;

#[derive(Debug, Serialize, Clone)]
enum OpType {
    #[serde(rename = "read")]
    Read,
    #[serde(rename = "write")]
    Write,
}

// Example CSV format:
// client,op_id,req_time,res_time,op_type,key,value,result
// c1,17,1772727389932,1772727390936,write,x,42,ok
// c1,18,1772727389942,1772727390936,read,x,,42
#[derive(Debug, Serialize, Clone)]
struct RequestData {
    client: NodeId,
    op_id: CommandId,
    req_time: Timestamp,
    res_time: Option<Timestamp>,
    op_type: OpType,
    key: String,
    value: Option<String>,
    result: Option<String>,
}

pub struct ClientData {
    request_data: Vec<RequestData>,
    response_count: usize,
}

impl ClientData {
    pub fn new() -> Self {
        ClientData {
            request_data: Vec::new(),
            response_count: 0,
        }
    }

    pub fn new_request(
        &mut self,
        client_id: ClientId,
        request_id: CommandId,
        is_write: bool,
        key: String,
        value: String,
    ) {
        let data = RequestData {
            client: client_id,
            op_id: request_id,
            req_time: Utc::now().timestamp_millis(),
            res_time: None, // Will be set when response is received
            op_type: if is_write {
                OpType::Write
            } else {
                OpType::Read
            },
            key,
            value: if is_write { Some(value) } else { None },
            result: None, // Will be replaced with actual result if read operation, or "ok" if write operation when response is received
        };
        self.request_data.push(data);
    }

    pub fn new_response(&mut self, req_index: usize, result: Option<String>) {
        let response_time = Utc::now().timestamp_millis();
        self.request_data[req_index].res_time = Some(response_time);
        self.request_data[req_index].result = result;
        self.response_count += 1;
    }

    pub fn response_count(&self) -> usize {
        self.response_count
    }

    pub fn request_count(&self) -> usize {
        self.request_data.len()
    }

    pub fn save_summary(&self, config: ClientConfig) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&config)?;
        let mut summary_file = File::create(config.summary_filepath)?;
        summary_file.write_all(config_json.as_bytes())?;
        summary_file.flush()?;
        Ok(())
    }

    pub fn to_csv(&self, file_path: String) -> Result<(), std::io::Error> {
        let file = File::create(file_path)?;
        let mut writer = Writer::from_writer(file);
        for data in &self.request_data {
            writer.serialize(data)?;
        }
        writer.flush()?;
        Ok(())
    }
}
