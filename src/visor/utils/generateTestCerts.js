const forge = require('node-forge');
const fs    = require('fs');
const path  = require('path');

async function main() {
  const RFC      = 'XAXX010101000';
  const PASSWORD = 'Prueba123!';
  const outDir   = path.join(__dirname, 'test-certs');

  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  console.log('Generando par de llaves RSA 2048...');
  const keys = forge.pki.rsa.generateKeyPair(2048);
  const cert = forge.pki.createCertificate();

  cert.publicKey    = keys.publicKey;
  cert.serialNumber = '01';
  cert.validity.notBefore = new Date();
  cert.validity.notAfter  = new Date();
  cert.validity.notAfter.setFullYear(cert.validity.notBefore.getFullYear() + 4);

  const attrs = [
    { name: 'commonName',         value: RFC },
    { name: 'organizationName',   value: 'PRUEBA' },
    { shortName: 'OU',            value: RFC },
  ];
  cert.setSubject(attrs);
  cert.setIssuer(attrs);
  cert.sign(keys.privateKey, forge.md.sha256.create());

  // Exportar .cer como DER puro
  const cerDer = forge.asn1.toDer(forge.pki.certificateToAsn1(cert));
  const cerBuf = Buffer.from(cerDer.getBytes(), 'binary');
  fs.writeFileSync(path.join(outDir, RFC + '.cer'), cerBuf);

  const privateKeyInfo = forge.pki.wrapRsaPrivateKey(
    forge.pki.privateKeyToAsn1(keys.privateKey)
  );
  const encryptedKeyAsn1 = forge.pki.encryptPrivateKeyInfo(
    privateKeyInfo, PASSWORD, { algorithm: '3des' }
  );
  const keyDerStr = forge.asn1.toDer(encryptedKeyAsn1).getBytes();
  const keyBuf    = Buffer.from(keyDerStr, 'binary');

  // Verificar que son bytes binarios reales, no base64
  console.log('Primeros 4 bytes del .key (deben ser 30 82):', keyBuf.slice(0, 4).toString('hex'));
  fs.writeFileSync(path.join(outDir, RFC + '.key'), keyBuf);

  console.log('─────────────────────────────────────');
  console.log('Archivos generados:');
  console.log('  RFC:        ' + RFC);
  console.log('  Contraseña: ' + PASSWORD);
  console.log('  .cer:       src/utils/test-certs/' + RFC + '.cer');
  console.log('  .key:       src/utils/test-certs/' + RFC + '.key');
  console.log('─────────────────────────────────────');
}

main().catch(console.error);
