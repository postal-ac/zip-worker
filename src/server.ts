// src/server.ts
import 'dotenv/config';
import { createServer, IncomingMessage, ServerResponse } from 'http';
import { ZipBuilder, ZipFileDescriptor } from './storage/zip-builder';

const PORT = Number(process.env.PORT || 4005);
const API_KEY = process.env.ZIP_SERVICE_API_KEY || '';

if (!API_KEY) {
  console.warn(
    '[zip-service] WARNING: ZIP_SERVICE_API_KEY is not set. Service will accept all requests.',
  );
}

const zipBuilder = new ZipBuilder({
  endpoint: process.env.WASABI_ENDPOINT!,
  region: process.env.WASABI_REGION!,
  bucket: process.env.WASABI_BUCKET_NAME!,
 accessKeyId: process.env.STORAGE_ACCESS_KEY_ID!,       
  secretAccessKey: process.env.STORAGE_SECRET_ACCESS_KEY!, 
});

type ZipRequestBody = {
  userId: string;
  zipName: string;
  files: ZipFileDescriptor[];
};

function readJsonBody(req: IncomingMessage): Promise<any> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      try {
        const raw = Buffer.concat(chunks).toString('utf8');
        const parsed = raw ? JSON.parse(raw) : {};
        resolve(parsed);
      } catch (err) {
        reject(err);
      }
    });
    req.on('error', reject);
  });
}

function unauthorized(res: ServerResponse) {
  res.statusCode = 401;
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify({ error: 'Unauthorized' }));
}

function checkApiKey(req: IncomingMessage): boolean {
  // Simple header-based API key
  if (!API_KEY) return true; // if you want "key optional" in dev

  const headerKey =
    (req.headers['x-zip-service-key'] as string | undefined) ||
    (req.headers['x-api-key'] as string | undefined);

  // constant-time compare would be ideal, but this is already decent for a private service
  return headerKey === API_KEY;
}

async function handleRequest(req: IncomingMessage, res: ServerResponse) {
  // health check stays open (optionally you could protect it too)
  if (req.method === 'GET' && req.url === '/health') {
    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ status: 'ok' }));
    return;
  }

  // All other endpoints require API key
  if (!checkApiKey(req)) {
    return unauthorized(res);
  }

  if (req.method === 'POST' && req.url === '/zip') {
    try {
      const body = (await readJsonBody(req)) as ZipRequestBody;

      if (!body.userId || !body.zipName || !Array.isArray(body.files)) {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'application/json');
        res.end(
          JSON.stringify({
            error: 'Invalid payload. Expected { userId, zipName, files[] }',
          }),
        );
        return;
      }

      console.log('[zip-service] /zip request', {
        userId: body.userId,
        zipName: body.zipName,
        fileCount: body.files.length,
      });

      const finalKey = await zipBuilder.buildZip({
        userId: body.userId,
        zipName: body.zipName,
        files: body.files,
      });

      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ success: true, finalKey }));
    } catch (err: any) {
      console.error('[zip-service] error', err);
      res.statusCode = 500;
      res.setHeader('Content-Type', 'application/json');
      res.end(
        JSON.stringify({
          success: false,
          error: err?.message ?? 'Internal error',
        }),
      );
    }
    return;
  }

  // Fallback 404
  res.statusCode = 404;
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify({ error: 'Not found' }));
}

const server = createServer(handleRequest);

server.listen(PORT, () => {
  console.log(`[zip-service] listening on http://localhost:${PORT}`);
});