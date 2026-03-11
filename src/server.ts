// scraper/src/server.ts
// Lightweight HTTP server for on-demand scrape triggers from Limor admin menu

import express from 'express';

const app = express();
app.use(express.json());

const API_KEY = process.env.SCRAPER_API_KEY;

// Auth middleware
app.use('/api', (req, res, next) => {
  const auth = req.headers.authorization;
  if (!API_KEY || auth !== `Bearer ${API_KEY}`) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }
  next();
});

app.post('/api/scrape', async (req, res) => {
  const { group_id } = req.body;

  // TODO: Spawn scraper process for the specified group (or all groups)
  // This will be implemented when the skill is finalized
  console.log(`Scrape triggered for: ${group_id ?? 'all'}`);

  res.json({ status: 'started', target: group_id ?? 'all' });
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Scraper server listening on port ${PORT}`);
});
