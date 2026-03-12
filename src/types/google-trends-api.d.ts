declare module 'google-trends-api' {
  interface Options {
    keyword: string | string[];
    startTime?: Date;
    endTime?: Date;
    geo?: string;
    hl?: string;
    category?: number;
    resolution?: string;
  }
  function interestOverTime(options: Options): Promise<string>;
  function interestByRegion(options: Options): Promise<string>;
  function relatedQueries(options: Options): Promise<string>;
  function relatedTopics(options: Options): Promise<string>;
  function dailyTrends(options: { geo: string }): Promise<string>;
  function realTimeTrends(options: { geo: string; category: string }): Promise<string>;
}
