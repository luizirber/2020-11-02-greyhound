(window.webpackJsonp=window.webpackJsonp||[]).push([[2],{1:function(e,t,n){"use strict";(function(e){const t=n(73);var r=n(75),o=n(87).FASTQStream,a=n(94),i=n(123),c=n(134);const d=n(137),s=n(146),l=document.querySelector("#drag-container"),u=document.querySelector("#progress-bar"),p=document.querySelector("#download_btn"),f=document.querySelector("#results-container");let h,m=0,b=0;const g=()=>l.classList.add("dragging"),w=()=>l.classList.remove("dragging");function v(){return c((function(e,t){return(e=>31===e[0]&&139===e[1])(e)?t(null,new i.Unzip):t(null,d())}))}function _(){return c((function(t,n){return(e=>">"===e.toString().charAt(0))(t)?n(null,s.obj(a(),d.obj((function(t,n,r){e.isBuffer(t)&&(t=t.toString()),JSON.parse(t),this.push(JSON.parse(t)),r()}),(function(){this.push(null)})))):(e=>"@"===e.toString().charAt(0))(t)?n(null,new o):void n(new Error("No parser available"))}))}function y(e){w(),e.preventDefault(),p.disabled=!0,u.style.transform="translateX(-100%)";var n=e.dataTransfer.files[0],o=new r(n);m=n.size,h=n.name,o.reader.onprogress=e=>{b+=e.loaded;let t=100-b/m*100;u.style.transform=`translateX(${-t}%)`};var a=new t.ComputeParameters;a.scaled=BigInt(2e3);var c=new t.Signature(a),d=new _,s=new v;switch(d.on("data",(function(e){c.add_sequence_js(e.seq)})).on("end",(function(e){const t=c.to_json(),n=new window.Blob([t],{type:"application/octet-binary"}),r=window.URL.createObjectURL(n),o=document.createElement("a");o.setAttribute("href",r),o.setAttribute("download",h+".sig"),document.querySelectorAll("#download_btn a").forEach(e=>e.parentNode.removeChild(e)),p.appendChild(o),p.addEventListener("click",()=>{o.click()}),p.disabled=!1,u.style.transform="translateX(0%)",fetch("/gather",{method:"POST",body:t,headers:{"Content-Type":"application/json"}}).then(e=>e.json()).then(e=>{const t=document.createElement("table");let n=t.createTHead();for(const e of["overlap","p_query","p_match","name"]){let t=document.createElement("th"),r=document.createTextNode(e);t.appendChild(r),n.appendChild(t)}const r=function(e,t){return e.toFixed(t).replace(/\.?0*$/,"")};for(const n of e){let e,a,i=t.insertRow(-1);n.intersect_bp=(o=n.intersect_bp)<500?r(o,0)+" bp":o<=5e5?r(o/1e3,1)+" Kbp":o<5e8?r(o/1e6,1)+" Mbp":o<5e11?r(o/1e9,1)+" Gbp":"???",n.f_orig_query=r(100*n.f_orig_query,1)+"%",n.f_match=r(100*n.f_match,1)+"%",n.average_abund=r(n.average_abund,1);for(const t of["intersect_bp","f_orig_query","f_match"])e=i.insertCell(-1),a=document.createTextNode(n[t]),e.appendChild(a);e=i.insertCell(-1);let c=new String(n.filename).substring(n.filename.lastIndexOf("/")+1);c=c.substring(0,c.length-4);const d=document.createElement("a");d.setAttribute("href","https://www.ncbi.nlm.nih.gov/assembly/"+c),a=document.createTextNode(n.name),d.appendChild(a),e.appendChild(d)}for(var o;f.firstChild;)f.removeChild(f.firstChild);f.appendChild(t)})})),n.type){case"application/gzip":o.pipe(new i.Unzip).pipe(d);break;default:o.pipe(s).pipe(d)}}l.addEventListener("dragenter",g),l.addEventListener("dragover",g),l.addEventListener("drop",y),l.addEventListener("dragleave",w)}).call(this,n(4).Buffer)},112:function(e,t){},115:function(e,t){},117:function(e,t){},139:function(e,t){},141:function(e,t){},148:function(e,t){},151:function(e,t){},153:function(e,t){},77:function(e,t){},79:function(e,t){},88:function(e,t){},90:function(e,t){}}]);