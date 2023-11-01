use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use hashring::HashRing;

use crate::streaming_layer::AgentInfo;


const N_VNODES: usize = 100;

#[derive(Debug, Clone)]
pub struct PhysicalNode {
    ip: String,
    port: u16,
}

#[derive(Debug, Copy, Clone, Hash, PartialEq)]
pub struct VNode {
    id: usize,
    addr: SocketAddr,
}

impl VNode {
    fn new(ip: &str, port: u16, id: usize) -> Self {
        let addr = SocketAddr::new(IpAddr::from_str(&ip).unwrap(), port);
        VNode { id, addr }
    }

    fn create_vnodes(ip: &str, port: u16) -> Vec<VNode> {
        let mut vnodes = vec![];
        for i in 0..N_VNODES {
            vnodes.push(VNode::new(ip, port, i));
        }
        vnodes
    }
}

impl ToString for VNode {
    fn to_string(&self) -> String {
        format!("{}|{}", self.addr, self.id)
    }
}

pub struct CHRing {
    ring: HashRing<VNode>,
}

impl CHRing {
    pub fn new() -> Self {
        CHRing {
            ring: HashRing::new(),
        }
    }

    pub fn add(&mut self, physical_node: PhysicalNode) {
        let vnodes = VNode::create_vnodes(&physical_node.ip, physical_node.port);
        for vnode in vnodes {
            self.ring.add(vnode);
        }
    }

    pub fn remove(&mut self, physical_node: PhysicalNode) {
        let vnodes = VNode::create_vnodes(&physical_node.ip, physical_node.port);
        for vnode in vnodes {
            self.ring.remove(&vnode);
        }
    }

    pub fn get(&self, key: &str) -> Option<&VNode> {
        self.ring.get(&key)
    }
}

impl From<Vec<PhysicalNode>> for CHRing {
    fn from(physical_nodes: Vec<PhysicalNode>) -> Self {
        let mut ring = CHRing::new();
        for physical_node in physical_nodes {
            ring.add(physical_node);
        }
        ring
    }
}

impl From<&[PhysicalNode]> for CHRing {
    fn from(physical_nodes: &[PhysicalNode]) -> Self {
        let mut ring = CHRing::new();
        for physical_node in physical_nodes {
            ring.add(physical_node.clone());
        }
        ring
    }
}

impl From<&HashSet<AgentInfo>> for CHRing {
    fn from(agents: &HashSet<AgentInfo>) -> Self {
        let mut ring = CHRing::new();
        for agent in agents {
            let physical_node = PhysicalNode {
                ip: agent.service_url.ip().to_string(),
                port: agent.service_url.port(),
            };
            ring.add(physical_node);
        }
        ring
    }
}

// fn main() {
//     let mut physical_nodes = Vec::with_capacity(100 * 100);

//     for i in 0..100 {
//         let physical_node = PhysicalNode {
//             ip: format!("127.0.0.{}", i),
//             port: 8080,
//         };
//         physical_nodes.push(physical_node);
//     }

//     let start = std::time::Instant::now();
//     let mut ring = CHRing::from(physical_nodes);
//     println!("build time: {:?}", start.elapsed());

//     let start = std::time::Instant::now();
//     let p_node = PhysicalNode {
//         ip: "127.0.0.100".to_string(),
//         port: 8080,
//     };
//     ring.add(p_node);
//     println!("add time: {:?}", start.elapsed());

//     let start = std::time::Instant::now();
//     let p_node = PhysicalNode {
//         ip: "127.0.0.100".to_string(),
//         port: 8080,
//     };
//     ring.remove(p_node);
//     println!("remove time: {:?}", start.elapsed());
// }

// key could be topic_name, partition, section number, metadata number
