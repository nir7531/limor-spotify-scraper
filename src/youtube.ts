// scraper/src/youtube.ts
// YouTube Data API v3 + YouTube Analytics API v2 client
// All analytics calls use timezone=Asia/Jerusalem

import { google, youtube_v3 } from 'googleapis';
import { getAuthForChannel, getOAuth2Client } from './youtube-auth.js';
import { trackUnits, canProceed } from './quota.js';
import { insertYouTubeScanData } from './youtube-db.js';
import { queryOne } from './db.js';

const logger = {
  info: (msg: string, ...args: any[]) => console.log(`[youtube] ${msg}`, ...args),
  warn: (msg: string, ...args: any[]) => console.warn(`[youtube] WARN ${msg}`, ...args),
  error: (msg: string, ...args: any[]) => console.error(`[youtube] ERROR ${msg}`, ...args),
};

// ---- Period calculation ----

const TIMEZONE = 'Asia/Jerusalem';

function getDateRange(days: number | null, channelCreatedAt?: string): { startDate: string; endDate: string } {
  const now = new Date();
  const endDate = now.toISOString().split('T')[0];
  if (days === null) {
    const start = channelCreatedAt
      ? channelCreatedAt.split('T')[0]
      : new Date(now.getTime() - 5 * 365 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    return { startDate: start, endDate };
  }
  const start = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
  return { startDate: start.toISOString().split('T')[0], endDate };
}

const PERIOD_DAYS: Record<string, number | null> = {
  '7d': 7,
  '30d': 28,  // YouTube uses 28-day window, we label it 30d
  '90d': 90,
  'all_time': null,
};

// ---- Internal types (API responses) ----

export interface ChannelOverview {
  views: number;
  watch_hours: number;
  subscribers_delta: number;
  impressions: number;
  ctr: number;
  avg_view_duration_seconds: number;
  avg_view_pct: number;
  subscriber_views_pct: number;
  non_subscriber_views_pct: number;
  new_viewers_pct: number;
  returning_viewers_pct: number;
  likes: number;
  comments: number;
  date_range: string;
}

export interface ShortsOverview {
  views: number;
  engaged_views: number;
  watch_hours: number;
  avg_view_duration_seconds: number;
  likes: number;
  comments: number;
  date_range: string;
}

export interface EpisodeStats {
  video_id: string;
  title: string;
  published_at: string;
  views: number;
  watch_hours: number;
  likes: number;
  comments: number;
  rank: number;
}

export interface Demographics {
  male_pct: number;
  female_pct: number;
  age_13_17: number;
  age_18_24: number;
  age_25_34: number;
  age_35_44: number;
  age_45_54: number;
  age_55_64: number;
  age_65_plus: number;
}

export interface TrafficSources {
  search_pct: number;
  suggested_pct: number;
  browse_pct: number;
  external_pct: number;
  playlist_pct: number;
  other_pct: number;
}

export interface GeoEntry {
  rank: number;
  country: string;
  percentage: number;
}

export interface DeviceTypes {
  mobile_pct: number;
  desktop_pct: number;
  tv_pct: number;
  tablet_pct: number;
}

export interface YouTubeScanResult {
  overview: Record<string, ChannelOverview>;
  shorts: Record<string, ShortsOverview>;
  episodes: EpisodeStats[];
  demographics: Record<string, Demographics>;
  trafficSources: Record<string, TrafficSources>;
  geo: GeoEntry[];
  deviceTypes: Record<string, DeviceTypes>;
  channelTitle: string;
}

export interface YouTubeGroup {
  id: string;
  youtube_channel_id: string;
  youtube_podcast_playlist_id: string | null;
  client_name: string;
}

// ---- API client helpers ----

function getYouTubeDataClient(auth: InstanceType<typeof google.auth.OAuth2>): youtube_v3.Youtube {
  return google.youtube({ version: 'v3', auth });
}

function getYouTubeAnalyticsClient(auth: InstanceType<typeof google.auth.OAuth2>) {
  return google.youtubeAnalytics({ version: 'v2', auth });
}

/**
 * Wrapper for analytics.reports.query.
 * The `timezone` parameter exists in the API but is not in the TS type definitions,
 * so we use `as any` to pass it through.
 */
async function analyticsQuery(
  analytics: ReturnType<typeof getYouTubeAnalyticsClient>,
  params: {
    ids: string;
    startDate: string;
    endDate: string;
    metrics: string;
    dimensions?: string;
    filters?: string;
    sort?: string;
    maxResults?: number;
  }
): Promise<{ rows: any[][] | null; columnHeaders: Array<{ name?: string | null }> | null }> {
  const res = await analytics.reports.query({
    ...params,
    timezone: TIMEZONE,
  } as any);
  const data = (res as any).data;
  return {
    rows: data?.rows ?? null,
    columnHeaders: data?.columnHeaders ?? null,
  };
}

// ---- Channel info ----

async function getChannelInfo(
  auth: InstanceType<typeof google.auth.OAuth2>,
  channelId: string
): Promise<{ title: string; createdAt: string } | null> {
  const yt = getYouTubeDataClient(auth);
  trackUnits(1);
  const res = await yt.channels.list({
    id: [channelId],
    part: ['snippet'],
  });
  const channel = res.data.items?.[0];
  if (!channel) return null;
  return {
    title: channel.snippet?.title ?? '',
    createdAt: channel.snippet?.publishedAt ?? '',
  };
}

// ---- Playlist discovery ----

export async function findPodcastPlaylist(
  auth: InstanceType<typeof google.auth.OAuth2>,
  channelId: string
): Promise<string | null> {
  const yt = getYouTubeDataClient(auth);
  let pageToken: string | undefined;

  do {
    trackUnits(1);
    const res = await yt.playlists.list({
      channelId,
      part: ['id', 'snippet', 'status'],
      maxResults: 50,
      pageToken,
    } as youtube_v3.Params$Resource$Playlists$List);

    for (const pl of res.data.items ?? []) {
      const title = (pl.snippet?.title ?? '').toLowerCase();
      const desc = (pl.snippet?.description ?? '').toLowerCase();
      if (title.includes('podcast') || title.includes('פודקאסט') || desc.includes('podcast')) {
        return pl.id ?? null;
      }
    }

    pageToken = res.data.nextPageToken ?? undefined;
  } while (pageToken);

  return null;
}

export async function getPlaylistVideoIds(
  auth: InstanceType<typeof google.auth.OAuth2>,
  playlistId: string
): Promise<Array<{ videoId: string; title: string; publishedAt: string }>> {
  const yt = getYouTubeDataClient(auth);
  const MAX_PAGES = 10;
  const items: Array<{ videoId: string; title: string; publishedAt: string }> = [];
  let pageToken: string | undefined;
  let page = 0;

  do {
    trackUnits(1);
    const res = await yt.playlistItems.list({
      playlistId,
      part: ['snippet', 'contentDetails'],
      maxResults: 50,
      pageToken,
    });

    for (const item of res.data.items ?? []) {
      const videoId = item.contentDetails?.videoId ?? item.snippet?.resourceId?.videoId;
      if (!videoId) continue;
      items.push({
        videoId,
        title: item.snippet?.title ?? '',
        publishedAt: item.snippet?.publishedAt?.split('T')[0] ?? '',
      });
    }

    pageToken = res.data.nextPageToken ?? undefined;
    page++;
  } while (pageToken && page < MAX_PAGES);

  return items;
}

// ---- Analytics API calls ----

export async function fetchChannelAnalytics(
  auth: InstanceType<typeof google.auth.OAuth2>,
  channelId: string,
  period: string,
  channelCreatedAt: string
): Promise<ChannelOverview | null> {
  if (!canProceed()) {
    logger.warn('Quota exhausted, skipping channel analytics');
    return null;
  }

  const days = PERIOD_DAYS[period];
  const { startDate, endDate } = getDateRange(days, channelCreatedAt);
  const analytics = getYouTubeAnalyticsClient(auth);
  const ids = `channel==${channelId}`;

  // Fetch views, watch time, likes, comments
  trackUnits(1);
  const mainData = await analyticsQuery(analytics, {
    ids, startDate, endDate,
    metrics: 'views,estimatedMinutesWatched,likes,comments',
  });
  const mainRow = mainData.rows?.[0];

  // Fetch impressions and CTR
  let impressions = 0;
  let ctr = 0;
  try {
    trackUnits(1);
    const impData = await analyticsQuery(analytics, {
      ids, startDate, endDate,
      metrics: 'impressions,impressionsClickThroughRate',
    });
    const impRow = impData.rows?.[0];
    if (impRow) {
      impressions = Number(impRow[0] ?? 0);
      ctr = Number(impRow[1] ?? 0) * 100; // API returns 0-1, convert to %
    }
  } catch (err) {
    logger.warn(`Impressions fetch failed for ${channelId} (${period}):`, err);
  }

  // Fetch avg view duration and avg view pct
  let avgDurationSec = 0;
  let avgViewPct = 0;
  try {
    trackUnits(1);
    const durData = await analyticsQuery(analytics, {
      ids, startDate, endDate,
      metrics: 'averageViewDuration,averageViewPercentage',
    });
    const durRow = durData.rows?.[0];
    if (durRow) {
      avgDurationSec = Number(durRow[0] ?? 0);
      avgViewPct = Number(durRow[1] ?? 0);
    }
  } catch (err) {
    logger.warn(`Duration fetch failed for ${channelId} (${period}):`, err);
  }

  // Fetch subscriber split (SUBSCRIBED vs NOT_SUBSCRIBED)
  let subscriberViewsPct = 0;
  let nonSubscriberViewsPct = 0;
  try {
    trackUnits(1);
    const subData = await analyticsQuery(analytics, {
      ids, startDate, endDate,
      metrics: 'views',
      dimensions: 'subscribedStatus',
    });
    const rows = subData.rows ?? [];
    const totalViews = rows.reduce((sum, r) => sum + Number(r[1] ?? 0), 0);
    if (totalViews > 0) {
      for (const row of rows) {
        const pct = Number(row[1] ?? 0) / totalViews * 100;
        if (row[0] === 'SUBSCRIBED') subscriberViewsPct = pct;
        else nonSubscriberViewsPct = pct;
      }
    }
  } catch (err) {
    logger.warn(`Subscriber split fetch failed for ${channelId} (${period}):`, err);
  }

  // Fetch new vs returning viewers
  let newViewersPct = 0;
  let returningViewersPct = 0;
  try {
    trackUnits(1);
    const viewerData = await analyticsQuery(analytics, {
      ids, startDate, endDate,
      metrics: 'views',
      dimensions: 'newVsReturningViewers',
    });
    const rows = viewerData.rows ?? [];
    const totalViews = rows.reduce((sum, r) => sum + Number(r[1] ?? 0), 0);
    if (totalViews > 0) {
      for (const row of rows) {
        const pct = Number(row[1] ?? 0) / totalViews * 100;
        if (row[0] === 'NEW') newViewersPct = pct;
        else returningViewersPct = pct;
      }
    }
  } catch (err) {
    logger.warn(`New vs returning fetch failed for ${channelId} (${period}):`, err);
  }

  // Fetch subscribers delta
  let subscribersDelta = 0;
  try {
    trackUnits(1);
    const subDeltaData = await analyticsQuery(analytics, {
      ids, startDate, endDate,
      metrics: 'subscribersGained,subscribersLost',
    });
    const subRow = subDeltaData.rows?.[0];
    if (subRow) {
      subscribersDelta = Number(subRow[0] ?? 0) - Number(subRow[1] ?? 0);
    }
  } catch (err) {
    logger.warn(`Subscribers delta fetch failed for ${channelId} (${period}):`, err);
  }

  const views = Number(mainRow?.[0] ?? 0);
  const watchMinutes = Number(mainRow?.[1] ?? 0);
  const likes = Number(mainRow?.[2] ?? 0);
  const comments = Number(mainRow?.[3] ?? 0);

  return {
    views,
    watch_hours: Math.round(watchMinutes / 60 * 10) / 10,
    subscribers_delta: subscribersDelta,
    impressions,
    ctr: Math.round(ctr * 100) / 100,
    avg_view_duration_seconds: Math.round(avgDurationSec),
    avg_view_pct: Math.round(avgViewPct * 10) / 10,
    subscriber_views_pct: Math.round(subscriberViewsPct * 10) / 10,
    non_subscriber_views_pct: Math.round(nonSubscriberViewsPct * 10) / 10,
    new_viewers_pct: Math.round(newViewersPct * 10) / 10,
    returning_viewers_pct: Math.round(returningViewersPct * 10) / 10,
    likes,
    comments,
    date_range: `${startDate}..${endDate}`,
  };
}

export async function fetchShortsAnalytics(
  auth: InstanceType<typeof google.auth.OAuth2>,
  channelId: string,
  period: string,
  channelCreatedAt: string
): Promise<ShortsOverview | null> {
  if (!canProceed()) return null;

  const days = PERIOD_DAYS[period];
  const { startDate, endDate } = getDateRange(days, channelCreatedAt);
  const analytics = getYouTubeAnalyticsClient(auth);
  const ids = `channel==${channelId}`;

  try {
    trackUnits(1);
    const mainData = await analyticsQuery(analytics, {
      ids, startDate, endDate,
      metrics: 'views,estimatedMinutesWatched,likes,comments,averageViewDuration',
      filters: 'creatorContentType==SHORTS',
    });

    let engagedViews = 0;
    try {
      trackUnits(1);
      const engData = await analyticsQuery(analytics, {
        ids, startDate, endDate,
        metrics: 'shorts_engagedViews',
        filters: 'creatorContentType==SHORTS',
      });
      engagedViews = Number(engData.rows?.[0]?.[0] ?? 0);
    } catch {
      // shorts_engagedViews may not be available for all channels
    }

    const row = mainData.rows?.[0];
    if (!row) return null;

    return {
      views: Number(row[0] ?? 0),
      engaged_views: engagedViews,
      watch_hours: Math.round(Number(row[1] ?? 0) / 60 * 10) / 10,
      avg_view_duration_seconds: Math.round(Number(row[4] ?? 0)),
      likes: Number(row[2] ?? 0),
      comments: Number(row[3] ?? 0),
      date_range: `${startDate}..${endDate}`,
    };
  } catch (err) {
    logger.warn(`Shorts analytics failed for ${channelId} (${period}):`, err);
    return null;
  }
}

export async function fetchEpisodeAnalytics(
  auth: InstanceType<typeof google.auth.OAuth2>,
  channelId: string,
  videoItems: Array<{ videoId: string; title: string; publishedAt: string }>
): Promise<EpisodeStats[]> {
  if (!canProceed() || videoItems.length === 0) return [];

  const analytics = getYouTubeAnalyticsClient(auth);
  const MAX_FILTER = 500;
  const allStats: EpisodeStats[] = [];

  for (let i = 0; i < videoItems.length; i += MAX_FILTER) {
    const batch = videoItems.slice(i, i + MAX_FILTER);
    const videoFilter = `video==${batch.map(v => v.videoId).join(',')}`;

    try {
      trackUnits(1);
      const resData = await analyticsQuery(analytics, {
        ids: `channel==${channelId}`,
        startDate: '2020-01-01',
        endDate: new Date().toISOString().split('T')[0],
        metrics: 'views,estimatedMinutesWatched,likes,comments',
        dimensions: 'video',
        filters: videoFilter,
        sort: '-views',
        maxResults: MAX_FILTER,
      });

      const headers = resData.columnHeaders?.map(h => h.name ?? '') ?? [];

      for (const row of resData.rows ?? []) {
        const videoId = String(row[0] ?? '');
        const item = batch.find(v => v.videoId === videoId);
        if (!item) continue;

        const getMetric = (name: string) => {
          const idx = headers.indexOf(name);
          return idx >= 0 ? Number(row[idx] ?? 0) : 0;
        };

        allStats.push({
          video_id: videoId,
          title: item.title,
          published_at: item.publishedAt,
          views: getMetric('views'),
          watch_hours: Math.round(getMetric('estimatedMinutesWatched') / 60 * 10) / 10,
          likes: getMetric('likes'),
          comments: getMetric('comments'),
          rank: 0,
        });
      }
    } catch (err) {
      logger.warn(`Episode analytics batch failed for ${channelId}:`, err);
    }
  }

  allStats.sort((a, b) => b.views - a.views);
  allStats.forEach((ep, idx) => { ep.rank = idx + 1; });

  return allStats.slice(0, 50);
}

export async function fetchDemographics(
  auth: InstanceType<typeof google.auth.OAuth2>,
  channelId: string,
  period: string,
  channelCreatedAt: string
): Promise<Demographics | null> {
  if (period === 'all_time') return null;
  if (!canProceed()) return null;

  const days = PERIOD_DAYS[period];
  const { startDate, endDate } = getDateRange(days, channelCreatedAt);
  const analytics = getYouTubeAnalyticsClient(auth);

  try {
    trackUnits(1);
    const resData = await analyticsQuery(analytics, {
      ids: `channel==${channelId}`,
      startDate,
      endDate,
      metrics: 'viewerPercentage',
      dimensions: 'gender,ageGroup',
    });

    const rows = resData.rows ?? [];
    const demo: Demographics = {
      male_pct: 0, female_pct: 0,
      age_13_17: 0, age_18_24: 0, age_25_34: 0,
      age_35_44: 0, age_45_54: 0, age_55_64: 0, age_65_plus: 0,
    };

    // Gender totals (sum across all age groups)
    const genderTotals: Record<string, number> = {};
    for (const row of rows) {
      const gender = String(row[0] ?? '').toUpperCase();
      const pct = Number(row[2] ?? 0);
      genderTotals[gender] = (genderTotals[gender] ?? 0) + pct;
    }
    demo.male_pct = Math.round((genderTotals['MALE'] ?? 0) * 10) / 10;
    demo.female_pct = Math.round((genderTotals['FEMALE'] ?? 0) * 10) / 10;

    // Age group totals (sum across genders)
    const ageTotals: Record<string, number> = {};
    for (const row of rows) {
      const ageGroup = String(row[1] ?? '');
      const pct = Number(row[2] ?? 0);
      ageTotals[ageGroup] = (ageTotals[ageGroup] ?? 0) + pct;
    }
    demo.age_13_17 = Math.round((ageTotals['age13-17'] ?? 0) * 10) / 10;
    demo.age_18_24 = Math.round((ageTotals['age18-24'] ?? 0) * 10) / 10;
    demo.age_25_34 = Math.round((ageTotals['age25-34'] ?? 0) * 10) / 10;
    demo.age_35_44 = Math.round((ageTotals['age35-44'] ?? 0) * 10) / 10;
    demo.age_45_54 = Math.round((ageTotals['age45-54'] ?? 0) * 10) / 10;
    demo.age_55_64 = Math.round((ageTotals['age55-64'] ?? 0) * 10) / 10;
    demo.age_65_plus = Math.round((ageTotals['age65-'] ?? 0) * 10) / 10;

    return demo;
  } catch (err) {
    logger.warn(`Demographics fetch failed for ${channelId} (${period}):`, err);
    return null;
  }
}

export async function fetchTrafficSources(
  auth: InstanceType<typeof google.auth.OAuth2>,
  channelId: string,
  period: string,
  channelCreatedAt: string
): Promise<TrafficSources | null> {
  if (!canProceed()) return null;

  const days = PERIOD_DAYS[period];
  const { startDate, endDate } = getDateRange(days, channelCreatedAt);
  const analytics = getYouTubeAnalyticsClient(auth);

  try {
    trackUnits(1);
    const resData = await analyticsQuery(analytics, {
      ids: `channel==${channelId}`,
      startDate,
      endDate,
      metrics: 'views',
      dimensions: 'insightTrafficSourceType',
      sort: '-views',
    });

    const rows = resData.rows ?? [];
    const totalViews = rows.reduce((sum, r) => sum + Number(r[1] ?? 0), 0);

    const sourcePcts: Record<string, number> = {};
    if (totalViews > 0) {
      for (const row of rows) {
        const source = String(row[0] ?? '');
        sourcePcts[source] = Number(row[1] ?? 0) / totalViews * 100;
      }
    }

    const search = (sourcePcts['YT_SEARCH'] ?? 0) + (sourcePcts['SUBSCRIBER'] ?? 0);
    const suggested = sourcePcts['RELATED_VIDEO'] ?? 0;
    const browse = (sourcePcts['BROWSE_FEATURES'] ?? 0) + (sourcePcts['END_SCREEN'] ?? 0);
    const external = sourcePcts['EXTERNAL'] ?? 0;
    const playlist = sourcePcts['PLAYLIST'] ?? 0;
    const knownTotal = search + suggested + browse + external + playlist;
    const other = Math.max(0, 100 - knownTotal);

    return {
      search_pct: Math.round(search * 10) / 10,
      suggested_pct: Math.round(suggested * 10) / 10,
      browse_pct: Math.round(browse * 10) / 10,
      external_pct: Math.round(external * 10) / 10,
      playlist_pct: Math.round(playlist * 10) / 10,
      other_pct: Math.round(other * 10) / 10,
    };
  } catch (err) {
    logger.warn(`Traffic sources fetch failed for ${channelId} (${period}):`, err);
    return null;
  }
}

export async function fetchGeo(
  auth: InstanceType<typeof google.auth.OAuth2>,
  channelId: string,
  channelCreatedAt: string
): Promise<GeoEntry[]> {
  if (!canProceed()) return [];

  const { startDate, endDate } = getDateRange(90, channelCreatedAt);
  const analytics = getYouTubeAnalyticsClient(auth);

  try {
    trackUnits(1);
    const resData = await analyticsQuery(analytics, {
      ids: `channel==${channelId}`,
      startDate,
      endDate,
      metrics: 'views',
      dimensions: 'country',
      sort: '-views',
      maxResults: 5,
    });

    const rows = resData.rows ?? [];
    const totalViews = rows.reduce((sum, r) => sum + Number(r[1] ?? 0), 0);

    return rows.slice(0, 5).map((row, idx) => ({
      rank: idx + 1,
      country: String(row[0] ?? ''),
      // Store as 0-1 decimal (percentage convention from plan)
      percentage: totalViews > 0 ? Math.round(Number(row[1] ?? 0) / totalViews * 1000) / 1000 : 0,
    }));
  } catch (err) {
    logger.warn(`Geo fetch failed for ${channelId}:`, err);
    return [];
  }
}

export async function fetchDeviceTypes(
  auth: InstanceType<typeof google.auth.OAuth2>,
  channelId: string,
  period: string,
  channelCreatedAt: string
): Promise<DeviceTypes | null> {
  if (!canProceed()) return null;

  const days = PERIOD_DAYS[period];
  const { startDate, endDate } = getDateRange(days, channelCreatedAt);
  const analytics = getYouTubeAnalyticsClient(auth);

  try {
    trackUnits(1);
    const resData = await analyticsQuery(analytics, {
      ids: `channel==${channelId}`,
      startDate,
      endDate,
      metrics: 'views',
      dimensions: 'deviceType',
      sort: '-views',
    });

    const rows = resData.rows ?? [];
    const totalViews = rows.reduce((sum, r) => sum + Number(r[1] ?? 0), 0);

    const devicePcts: Record<string, number> = {};
    if (totalViews > 0) {
      for (const row of rows) {
        const device = String(row[0] ?? '').toLowerCase();
        devicePcts[device] = Number(row[1] ?? 0) / totalViews * 100;
      }
    }

    return {
      mobile_pct: Math.round((devicePcts['mobile'] ?? 0) * 10) / 10,
      desktop_pct: Math.round((devicePcts['desktop'] ?? 0) * 10) / 10,
      tv_pct: Math.round(((devicePcts['tv'] ?? 0) + (devicePcts['game_console'] ?? 0)) * 10) / 10,
      tablet_pct: Math.round((devicePcts['tablet'] ?? 0) * 10) / 10,
    };
  } catch (err) {
    logger.warn(`Device types fetch failed for ${channelId} (${period}):`, err);
    return null;
  }
}

// ---- Channel validation ----

export async function validateYouTubeChannel(
  channelUrl: string
): Promise<{ channelId: string; channelTitle: string; playlistId: string | null }> {
  const channelId = await resolveChannelId(channelUrl);
  if (!channelId) throw new Error(`Could not resolve channel ID from URL: ${channelUrl}`);

  const auth = getOAuth2Client();
  const yt = getYouTubeDataClient(auth);

  trackUnits(1);
  const channelRes = await yt.channels.list({
    id: [channelId],
    part: ['snippet'],
  });

  const channel = channelRes.data.items?.[0];
  if (!channel) throw new Error(`Channel ${channelId} not found`);

  const playlistId = await findPodcastPlaylist(auth, channelId);

  return {
    channelId,
    channelTitle: channel.snippet?.title ?? '',
    playlistId,
  };
}

async function resolveChannelId(url: string): Promise<string | null> {
  const auth = getOAuth2Client();
  const yt = getYouTubeDataClient(auth);

  try {
    const parsed = new URL(url);
    const path = parsed.pathname;

    // Direct channel ID: /channel/UCxxx
    if (path.startsWith('/channel/')) {
      return path.replace('/channel/', '').split('/')[0];
    }

    // Handle: /@handle
    if (path.startsWith('/@')) {
      const handle = path.replace('/@', '').split('/')[0];
      trackUnits(1);
      const res = await yt.channels.list({
        forHandle: handle,
        part: ['id'],
      });
      return res.data.items?.[0]?.id ?? null;
    }

    // Username: /c/name or /user/name
    const usernameMatch = path.match(/^\/(c|user)\/([^/]+)/);
    if (usernameMatch) {
      const username = usernameMatch[2];
      trackUnits(1);
      const res = await yt.channels.list({
        forUsername: username,
        part: ['id'],
      });
      return res.data.items?.[0]?.id ?? null;
    }
  } catch (err) {
    logger.warn(`Failed to resolve channel ID from URL ${url}:`, err);
  }

  return null;
}

// ---- Main scan orchestrator ----

export async function scanYouTubeChannel(
  groupId: string,
  channelId: string,
  playlistId: string | null
): Promise<YouTubeScanResult> {
  logger.info(`Scanning YouTube channel ${channelId} for group ${groupId}`);

  const auth = await getAuthForChannel(groupId);
  const channelInfo = await getChannelInfo(auth, channelId);
  const channelCreatedAt = channelInfo?.createdAt ?? '';
  const channelTitle = channelInfo?.title ?? '';

  const overview: Record<string, ChannelOverview> = {};
  const shorts: Record<string, ShortsOverview> = {};
  const demographics: Record<string, Demographics> = {};
  const trafficSources: Record<string, TrafficSources> = {};
  const deviceTypes: Record<string, DeviceTypes> = {};

  for (const period of ['7d', '30d', '90d', 'all_time'] as const) {
    if (!canProceed()) {
      logger.warn(`Quota exhausted after period ${period}, stopping`);
      break;
    }

    const [ov, sh, demo, traffic, device] = await Promise.allSettled([
      fetchChannelAnalytics(auth, channelId, period, channelCreatedAt),
      fetchShortsAnalytics(auth, channelId, period, channelCreatedAt),
      fetchDemographics(auth, channelId, period, channelCreatedAt),
      fetchTrafficSources(auth, channelId, period, channelCreatedAt),
      fetchDeviceTypes(auth, channelId, period, channelCreatedAt),
    ]);

    if (ov.status === 'fulfilled' && ov.value) overview[period] = ov.value;
    if (sh.status === 'fulfilled' && sh.value) shorts[period] = sh.value;
    if (demo.status === 'fulfilled' && demo.value) demographics[period] = demo.value;
    if (traffic.status === 'fulfilled' && traffic.value) trafficSources[period] = traffic.value;
    if (device.status === 'fulfilled' && device.value) deviceTypes[period] = device.value;
  }

  const geo = await fetchGeo(auth, channelId, channelCreatedAt).catch(() => []);

  let episodes: EpisodeStats[] = [];
  if (playlistId && canProceed()) {
    const videoItems = await getPlaylistVideoIds(auth, playlistId).catch(() => []);
    if (videoItems.length > 0) {
      episodes = await fetchEpisodeAnalytics(auth, channelId, videoItems).catch(() => []);
    }
  }

  logger.info(`YouTube scan complete for ${channelTitle}: ${Object.keys(overview).length} periods, ${episodes.length} episodes`);

  return { overview, shorts, episodes, demographics, trafficSources, geo, deviceTypes, channelTitle };
}

// ---- Batch orchestrator ----

export async function scanAllYouTube(
  groups: YouTubeGroup[]
): Promise<{ success: number; failed: number; failedGroups: string[] }> {
  let success = 0;
  const failedGroups: string[] = [];

  for (const group of groups) {
    if (!canProceed()) {
      logger.warn('YouTube API quota exhausted, stopping batch scan');
      break;
    }

    try {
      const result = await scanYouTubeChannel(
        group.id,
        group.youtube_channel_id,
        group.youtube_podcast_playlist_id
      );

      // Create a scan record with scan_source = 'youtube'
      const scanRow = await queryOne<{ id: string }>(
        `INSERT INTO podcast_scans (group_id, scan_number, show_name, scan_status, scan_source)
         VALUES ($1, (SELECT COALESCE(MAX(scan_number), 0) + 1 FROM podcast_scans WHERE group_id = $1), $2, 'completed', 'youtube')
         RETURNING id`,
        [group.id, group.client_name]
      );

      if (scanRow) {
        await insertYouTubeScanData(scanRow.id, result);
      }

      success++;
    } catch (err) {
      logger.error(`YouTube scan failed for ${group.client_name}:`, err);
      failedGroups.push(group.client_name);
    }
  }

  return { success, failed: failedGroups.length, failedGroups };
}
