const { google } = require('googleapis');
const fs = require('fs');
const { parseCFDI } = require('./src/services/cfdiParser');

const env = fs.readFileSync('.env', 'utf8');
const key = JSON.parse(env.match(/GOOGLE_SERVICE_ACCOUNT_KEY=(.*)/)[1]);
const auth = new google.auth.GoogleAuth({ credentials: key, scopes: ['https://www.googleapis.com/auth/drive.readonly'] });
const drive = google.drive({ version: 'v3', auth });

drive.files.list({
  q: "'1pvDIj6fy48LA6JthWvCpH-V5j-3GM24R' in parents and name contains '.xml' and trashed=false",
  fields: 'files(id,name)', pageSize: 5,
  supportsAllDrives: true, includeItemsFromAllDrives: true,
}).then(async r => {
  for (const f of r.data.files) {
    const dl = await drive.files.get({ fileId: f.id, alt: 'media', supportsAllDrives: true }, { responseType: 'arraybuffer' });
    const buf = Buffer.from(dl.data);
    const cfdi = await parseCFDI(buf.toString('utf8'));
    const d = new Date(cfdi.fecha);
    console.log(f.name, '| fecha:', cfdi.fecha, '| año:', d.getFullYear(), '| mes:', d.getMonth() + 1);
  }
}).catch(e => console.error(e.message));
