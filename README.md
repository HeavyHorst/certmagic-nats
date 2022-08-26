
# Certmagic Storage Backend for Nats KV

This library allows you to use [Nats KV](https://docs.nats.io/nats-concepts/jetstream/key-value-store) as a key/certificate storage backend for [Certmagic](https://github.com/caddyserver/certmagic).             

## Using it wit [Caddy](https://caddyserver.com/)

Enable the nats storage module in your Caddyfile:                                  

```
{ 
        storage nats {
                hosts "tls://nats01.example.com,tls://nats02.example.com,tls://nats03.example.com"      
                bucket "caddy_store"
                creds "/etc/caddy/caddy.creds"
                inbox_prefix "_CADDYINBOX"
                connection_name "caddy"
        }
} 

:443 {

}
```

## Nats permissions

Pub Allow:        
```                                                                
 $JS.API.CONSUMER.CREATE.KV_caddy_store                                                                               
 $JS.API.CONSUMER.DELETE.KV_caddy_store.>                                         
 $JS.API.STREAM.INFO.KV_caddy_store                                                
 $JS.API.STREAM.LIST                                                               
 $JS.API.STREAM.MSG.GET.KV_caddy_store                                             
 $JS.API.STREAM.NAMES                                                              
 $KV.caddy_store.>                     
```                                            
                                                                                   
Sub Allow                                                                        
  `_CADDYINBOX.>`
