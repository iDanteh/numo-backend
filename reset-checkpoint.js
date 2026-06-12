'use strict';

require('dotenv').config();
const mongoose = require('mongoose');

const RFC = 'CCO011113663';

mongoose.connect(process.env.MONGODB_URI).then(async () => {
  const r = await mongoose.connection.collection('satjobcheckpoints').updateMany(
    { rfc: RFC, status: { $in: ['solicitando', 'verificando', 'descargando'] } },
    { $set: { status: 'error', error: 'Reseteado manualmente', updatedAt: new Date() } }
  );
  console.log('Modificados:', r.modifiedCount);
  process.exit(0);
}).catch(err => { console.error(err); process.exit(1); });
