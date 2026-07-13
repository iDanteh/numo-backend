'use strict';

/**
 * Corre `promise` con un límite de tiempo. Si se excede `ms`, `onTimeout`
 * corre para liberar/reiniciar el recurso colgado (best-effort) y la promesa
 * devuelta se rechaza — pero la `promise` original NO se cancela (JS no puede
 * abortar una promesa en curso), sigue corriendo en segundo plano hasta que
 * resuelva o falle por su cuenta.
 */
function withTimeout(promise, ms, label, onTimeout) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      if (onTimeout) {
        try { onTimeout(); } catch { /* el cleanup nunca debe tapar el error de timeout */ }
      }
      reject(new Error(`${label}: excedió ${ms}ms sin responder`));
    }, ms);

    promise.then(
      (value) => { clearTimeout(timer); resolve(value); },
      (err)   => { clearTimeout(timer); reject(err); },
    );
  });
}

module.exports = { withTimeout };
