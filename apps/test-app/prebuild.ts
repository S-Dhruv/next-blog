import { cpSync, existsSync, mkdirSync } from 'fs';
import { join } from 'path';

const source = join(import.meta.dir, '../../packages/core/dist/nextjs/assets/@supergrowthai/next-blog-dashboard/static');
const dest = join(import.meta.dir, 'public/api/next-blog/dashboard/static');

if (existsSync(source)) {
    console.log('Copying dashboard static assets...');
    mkdirSync(dest, { recursive: true });
    cpSync(source, dest, { recursive: true });
} else {
    console.log('Warning: Dashboard static assets not found at', source);
}
