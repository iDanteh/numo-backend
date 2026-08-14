'use strict';

// resolvePrimeraIdentificacion — regla única compartida por todos los call-sites que
// pueden transicionar un BankMovement a status='identificado' (vinculación manual de
// CxC, ficha física, edición manual de status, motores de match automático por lotes).
// Puro y sin dependencias de Mongoose: recibe primitivos, no el doc completo, para que
// funcione igual con un doc .lean(), un doc Mongoose real, o un objeto plano de test.
//
// Regla de inmutabilidad: si `actual.primeraIdentificacionAt` ya tiene valor, se
// devuelve tal cual sin importar `nuevoStatus` — nunca se limpia al desvincular una
// CxC, revertir una reversión, o reclasificar. Solo se setea la PRIMERA vez que
// nuevoStatus es 'identificado' y todavía no había ningún valor.
function resolvePrimeraIdentificacion(nuevoStatus, actual, user) {
  const { primeraIdentificacionAt = null, primeraIdentificacionPor = null } = actual ?? {};
  if (nuevoStatus === 'identificado' && !primeraIdentificacionAt) {
    return {
      primeraIdentificacionAt: new Date(),
      primeraIdentificacionPor: {
        userId: user?._id ?? null,
        nombre: user?.nombre ?? null,
      },
    };
  }
  return { primeraIdentificacionAt, primeraIdentificacionPor };
}

module.exports = { resolvePrimeraIdentificacion };
