(()=>{
'use strict';
const DB_NAME='hmsarbide_offline_trabajo_github_20261117';
const DB_VERSION=2;
const Q='queue', B='blobs', S='state';
const CFG_KEY='hmsarbide_cloudflare_prepublicacion_v1_config';
// RECUPERACIÓN LOCAL: nunca contactar ni importar automáticamente un servidor remoto.
let dbp;
const listeners=new Set();
let syncing=false;
function openDB(){
  if(dbp)return dbp;
  dbp=new Promise((resolve,reject)=>{
    const r=indexedDB.open(DB_NAME,DB_VERSION);
    r.onupgradeneeded=()=>{
      const db=r.result;
      if(!db.objectStoreNames.contains(Q)){
        const s=db.createObjectStore(Q,{keyPath:'id'});
        s.createIndex('status','status',{unique:false});
        s.createIndex('updatedAt','updatedAt',{unique:false});
      }
      if(!db.objectStoreNames.contains(B))db.createObjectStore(B,{keyPath:'id'});
      if(!db.objectStoreNames.contains(S))db.createObjectStore(S,{keyPath:'id'});
    };
    r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);
  });
  return dbp;
}
function tx(store,mode='readonly'){return openDB().then(db=>db.transaction(store,mode).objectStore(store));}
function req(r){return new Promise((res,rej)=>{r.onsuccess=()=>res(r.result);r.onerror=()=>rej(r.error)});}
async function putQueue(item){const s=await tx(Q,'readwrite');await req(s.put(item));notify();}
async function enqueue(kind,payload,{dedupeKey='',blobId=''}={}){
  const now=new Date().toISOString();
  if(dedupeKey){
    const all=await listQueue();
    const old=all.find(x=>x.dedupeKey===dedupeKey&&x.status==='pending');
    if(old){old.payload=payload;old.updatedAt=now;old.blobId=blobId||old.blobId;await putQueue(old);return old.id;}
  }
  const id='q_'+Date.now().toString(36)+Math.random().toString(36).slice(2,7);
  await putQueue({id,kind,payload,dedupeKey,blobId,status:'pending',createdAt:now,updatedAt:now,tries:0,lastError:''});
  return id;
}
async function listQueue(){const s=await tx(Q);return (await req(s.getAll()))||[];}
async function pendingCount(){return (await listQueue()).filter(x=>x.status==='pending').length;}
async function storeBlob(blob,meta={}){
  const id='blob_'+Date.now().toString(36)+Math.random().toString(36).slice(2,8);
  const s=await tx(B,'readwrite');
  await req(s.put({id,blob,meta,createdAt:new Date().toISOString()}));
  return id;
}
async function getBlob(id){if(!id)return null;const s=await tx(B);return req(s.get(id));}
async function saveState(value){const s=await tx(S,'readwrite');await req(s.put({id:'main',value,updatedAt:new Date().toISOString()}));}
async function loadState(){const s=await tx(S);const rec=await req(s.get('main'));return rec?.value||null;}
async function deleteBlob(id){if(!id)return;const s=await tx(B,'readwrite');return req(s.delete(id));}
async function setStatus(id,status,lastError=''){const s=await tx(Q,'readwrite');const item=await req(s.get(id));if(!item)return;item.status=status;item.lastError=lastError;item.updatedAt=new Date().toISOString();if(status==='pending')item.tries=(item.tries||0)+1;await req(s.put(item));}
async function deleteQueue(id){const s=await tx(Q,'readwrite');await req(s.delete(id));notify();}
function getConfig(){return {endpoint:'',token:''};}
function configure(){throw new Error('Sincronización remota desactivada en esta versión local.');}
function clearConfig(){notify();}
function endpoint(){return '';}
function token(){return '';}
function authHeaders(extra={}){return {...extra};}
function fileToDataURL(blob){return new Promise((resolve,reject)=>{const r=new FileReader();r.onerror=()=>reject(r.error);r.onload=()=>resolve(r.result);r.readAsDataURL(blob);});}
async function hydratePhotos(remote,files){
  if(!remote||!Array.isArray(remote.services)||!Array.isArray(files))return remote;
  const ep=endpoint();
  const photos=files.filter(f=>f.kind==='photo');
  for(const f of photos){
    const svc=remote.services.find(s=>s.id===f.serviceId);if(!svc)continue;
    svc.photos=svc.photos||{};
    try{
      const resp=await fetch(`${ep}/file/${encodeURIComponent(f.id)}`,{headers:authHeaders()});
      if(!resp.ok)continue;
      const blob=await resp.blob();
      svc.photos[f.refKey]=await fileToDataURL(blob);
    }catch{}
  }
  const atts=files.filter(f=>f.kind==='attachment');
  for(const f of atts){
    const svc=remote.services.find(s=>s.id===f.serviceId);if(!svc)continue;
    svc.attachments=svc.attachments||[];
    let a=svc.attachments.find(x=>x.id===f.refKey);
    if(!a){a={id:f.refKey,name:f.fileName||'Documento',type:f.contentType||'application/octet-stream',size:f.size||0,createdAt:f.createdAt,syncStatus:'synced',remoteFileId:f.id};svc.attachments.push(a)}
    else{a.syncStatus='synced';a.remoteFileId=f.id;a.localOnly=false;}
  }
  return remote;
}
async function pullRemote(){return {updated:false,reason:'local-only'};}
async function syncNow(){return {synced:0,pending:await pendingCount(),reason:'local-only'};}
async function testConnection(){throw new Error('Sincronización remota desactivada.');}
async function getStatus(){const c=getConfig();return {online:navigator.onLine,pending:await pendingCount(),syncing,configured:!!(c.endpoint&&c.token),endpoint:c.endpoint||''};}
function onStatus(fn){listeners.add(fn);getStatus().then(fn);return()=>listeners.delete(fn);}
async function notify(){const st=await getStatus();listeners.forEach(fn=>{try{fn(st)}catch{}});}
window.addEventListener('online',()=>{notify();syncNow();});
window.addEventListener('offline',notify);
setInterval(()=>{if(navigator.onLine)syncNow();},45000);
openDB().then(notify).catch(()=>{});
window.MHSOffline={enqueue,storeBlob,getBlob,saveState,loadState,listQueue,pendingCount,syncNow,pullRemote,testConnection,getStatus,onStatus,endpoint,getConfig,configure,clearConfig};
})();
