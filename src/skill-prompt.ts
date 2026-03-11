// scraper/src/skill-prompt.ts
// Reads the skill content from skill-content.txt at startup

import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const skillPath = join(__dirname, '..', 'skill-content.txt');

export const SKILL_CONTENT = readFileSync(skillPath, 'utf-8');
