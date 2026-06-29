use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use futures::StreamExt;
use libp2p::{
    Multiaddr, SwarmBuilder,
    gossipsub::{self, IdentTopic},
    identify, identity, mdns, ping,
    swarm::{NetworkBehaviour, SwarmEvent},
};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let args = Args::parse();

    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = local_key.public().to_peer_id();
    info!(%local_peer_id, nick=%args.nick, "node starting");
    let topic = IdentTopic::new(args.topic.clone());
    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .validation_mode(gossipsub::ValidationMode::Strict)
        .heartbeat_interval(Duration::from_secs(3))
        .build()?;

    let mut gossipsub: gossipsub::Behaviour = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(local_key.clone()),
        gossipsub_config,
    )
    .expect("");
    gossipsub.subscribe(&topic)?;

    let behaviour = Behaviour {
        gossipsub,
        mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?,
        ping: ping::Behaviour::new(ping::Config::new().with_interval(Duration::from_secs(5))),
        identify: identify::Behaviour::new(identify::Config::new(
            "/tiny-p2p/0.1.0".into(),
            local_key.public(),
        )),
    };

    let mut swarm = SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default().nodelay(true),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|_| behaviour)?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(u64::MAX)))
        .build();

    swarm.listen_on(args.listen.clone())?;

    for addr in &args.dial {
        swarm.dial(addr.clone())?;
    }

    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let mut lines = tokio::io::AsyncBufReadExt::lines(stdin);

    let mut peers = std::collections::HashSet::new();

    loop {
        tokio::select! {
            line = lines.next_line() => {
                if let Ok(Some(line)) = line {
                    let payload = format!("{}: {}", args.nick, line);
                    if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), payload.as_bytes()) {
                        warn!("publish failed: {e}");
                    }
                }
            }

            event = swarm.select_next_some() => match event {
                libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } => {
                    info!("listening on {address}");
                }

                libp2p::swarm::SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, addr) in list {
                        peers.insert(peer_id);
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                        info!("mdns discovered {peer_id} at {addr}");
                    }
                }

                SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _) in list {
                        if peers.remove(&peer_id) {
                            swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                            info!("mdns expired {peer_id}");
                        }
                    }
                }

                SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(
                    gossipsub::Event::Message {
                        propagation_source,
                        message_id,
                        message,
                    }
                )) => {
                    let text = String::from_utf8_lossy(&message.data);
                    info!("msg from {propagation_source} id={message_id}: {text}");
                }

                SwarmEvent::Behaviour(BehaviourEvent::Identify(ev)) => {
                    info!("identify: {ev:?}");
                }

                SwarmEvent::Behaviour(BehaviourEvent::Ping(ev)) => {
                    info!("ping: {ev:?}");
                }

                _ => {}
            }
        }
    }
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    nick: String,
    #[arg(long, default_value = "tiny-net")]
    topic: String,
    #[arg(long, default_value = "/ip4/127.0.0.1/tcp/7001")]
    listen: Multiaddr,
    #[arg(long)]
    dial: Vec<Multiaddr>,
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
    ping: ping::Behaviour,
    identify: identify::Behaviour,
}
