(function(){
  window.__log = window.__log || [];
  var prior = window.vbpx && window.vbpx.q ? window.vbpx.q.slice() : [];
  window.vbpx = function(){ window.__log.push(Array.prototype.slice.call(arguments)); };
  for (var i=0;i<prior.length;i++) window.vbpx.apply(null, prior[i]);
  window.__vendorLoaded = true;
})();
