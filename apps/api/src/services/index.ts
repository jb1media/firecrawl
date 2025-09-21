import { createClient, SupabaseClient } from "@supabase/supabase-js";
import { logger as _logger } from "../lib/logger";
import { configDotenv } from "dotenv";
import { ApiError } from "@google-cloud/storage";
import crypto from "crypto";
import { redisEvictConnection } from "./redis";
import type { Logger } from "winston";
import psl from "psl";
import { MapDocument } from "../controllers/v2/types";
import { PdfMetadata } from "@mendable/firecrawl-rs";
import { storage } from "../lib/gcs-jobs";

configDotenv();

/**
 * -----------------------------
 *  Supabase Setup
 * -----------------------------
 */
class IndexSupabaseService {
  private client: SupabaseClient | null = null;

  constructor() {
    const supabaseUrl = process.env.INDEX_SUPABASE_URL;
    const supabaseServiceToken = process.env.INDEX_SUPABASE_SERVICE_TOKEN;
    if (!supabaseUrl || !supabaseServiceToken) {
      _logger.warn("Index supabase client will not be initialized.");
      this.client = null;
    } else {
      this.client = createClient(supabaseUrl, supabaseServiceToken);
    }
  }

  getClient(): SupabaseClient | null {
    return this.client;
  }
}

const serv = new IndexSupabaseService();

export const index_supabase_service: SupabaseClient = new Proxy(serv, {
  get: function (target, prop, receiver) {
    const client = target.getClient();
    if (client === null) {
      return () => {
        throw new Error("Index supabase client is not configured.");
      };
    }
    if (prop in target) {
      return Reflect.get(target, prop, receiver);
    }
    return Reflect.get(client, prop, receiver);
  },
}) as unknown as SupabaseClient;

export const useIndex =
  process.env.INDEX_SUPABASE_URL !== "" &&
  process.env.INDEX_SUPABASE_URL !== undefined;

/**
 * -----------------------------
 *  Helpers
 * -----------------------------
 */
function hasRedis() {
  return !!redisEvictConnection;
}

const credentials = process.env.GCS_CREDENTIALS
  ? JSON.parse(atob(process.env.GCS_CREDENTIALS))
  : undefined;

/**
 * -----------------------------
 *  GCS Helpers
 * -----------------------------
 */
export async function getIndexFromGCS(
  url: string,
  logger?: Logger,
): Promise<any | null> {
  try {
    if (!process.env.GCS_INDEX_BUCKET_NAME) {
      return null;
    }
    const bucket = storage.bucket(process.env.GCS_INDEX_BUCKET_NAME);
    const blob = bucket.file(`${url}`);
    const [blobContent] = await blob.download();
    const parsed = JSON.parse(blobContent.toString());

    try {
      if (typeof parsed.screenshot === "string") {
        const screenshotUrl = new URL(parsed.screenshot);
        let expiresAt =
          parseInt(screenshotUrl.searchParams.get("Expires") ?? "0", 10) * 1000;
        if (expiresAt === 0) {
          expiresAt =
            new Date(
              screenshotUrl.searchParams.get("X-Goog-Date") ??
                "1970-01-01T00:00:00Z",
            ).getTime() +
            parseInt(
              screenshotUrl.searchParams.get("X-Goog-Expires") ?? "0",
              10,
            ) *
              1000;
        }
        if (
          screenshotUrl.hostname === "storage.googleapis.com" &&
          expiresAt < Date.now()
        ) {
          logger?.info("Re-signing screenshot URL");
          const [url] = await storage
            .bucket(process.env.GCS_MEDIA_BUCKET_NAME!)
            .file(decodeURIComponent(screenshotUrl.pathname.split("/")[2]))
            .getSignedUrl({
              action: "read",
              expires: Date.now() + 1000 * 60 * 60 * 24 * 7,
            });
          parsed.screenshot = url;
          await blob.save(JSON.stringify(parsed), {
            contentType: "application/json",
          });
        }
      }
    } catch (error) {
      logger?.warn("Error re-signing screenshot URL", { error, url });
    }

    return parsed;
  } catch (error) {
    if (
      error instanceof ApiError &&
      error.code === 404 &&
      error.message.includes("No such object:")
    ) {
      return null;
    }
    (logger ?? _logger).error(`Error getting Index document from GCS`, {
      error,
      url,
    });
    return null;
  }
}

export async function saveIndexToGCS(
  id: string,
  doc: {
    url: string;
    html: string;
    statusCode: number;
    error?: string;
    screenshot?: string;
    pdfMetadata?: PdfMetadata;
    contentType?: string;
    postprocessorsUsed?: string[];
  },
): Promise<void> {
  try {
    if (!process.env.GCS_INDEX_BUCKET_NAME) {
      return;
    }
    const bucket = storage.bucket(process.env.GCS_INDEX_BUCKET_NAME);
    const blob = bucket.file(`${id}.json`);
    for (let i = 0; i < 3; i++) {
      try {
        await blob.save(JSON.stringify(doc), {
          contentType: "application/json",
        });
        break;
      } catch (error) {
        if (i === 2) {
          throw error;
        } else {
          _logger.error(`Error saving index document to GCS, retrying`, {
            error,
            indexId: id,
            i,
          });
        }
      }
    }
  } catch (error) {
    throw new Error("Error saving index document to GCS", { cause: error });
  }
}

/**
 * -----------------------------
 *  URL Helpers
 * -----------------------------
 */
export function normalizeURLForIndex(url: string): string {
  const urlObj = new URL(url);
  urlObj.hash = "";
  urlObj.protocol = "https";
  if (urlObj.port === "80" || urlObj.port === "443") urlObj.port = "";
  if (urlObj.hostname.startsWith("www.")) {
    urlObj.hostname = urlObj.hostname.slice(4);
  }
  if (urlObj.pathname.endsWith("/")) {
    urlObj.pathname = urlObj.pathname.slice(0, -1);
  }
  return urlObj.toString();
}

export function hashURL(url: string): string {
  return "\\x" + crypto.createHash("sha256").update(url).digest("hex");
}

export function generateURLSplits(url: string): string[] {
  const urls: string[] = [];
  const urlObj = new URL(url);
  urlObj.hash = "";
  urlObj.search = "";
  const pathnameParts = urlObj.pathname.split("/");
  for (let i = 0; i <= pathnameParts.length; i++) {
    urlObj.pathname = pathnameParts.slice(0, i).join("/");
    urls.push(urlObj.href);
  }
  urls.push(url);
  return [...new Set(urls.map(x => normalizeURLForIndex(x)))];
}

export function generateDomainSplits(
  hostname: string,
  fakeDomain?: string,
): string[] {
  const parsed = psl.parse(fakeDomain ?? hostname);
  if (!parsed || !parsed.domain) return [fakeDomain ?? hostname];
  const subdomains: string[] = (parsed.subdomain ?? "")
    .split(".")
    .filter(x => x !== "");
  const domains: string[] = [];
  for (let i = subdomains.length; i >= 0; i--) {
    domains.push(subdomains.slice(i).concat([parsed.domain]).join("."));
  }
  return domains;
}

/**
 * -----------------------------
 *  Redis + Supabase (Guarded)
 * -----------------------------
 */
export async function addIndexInsertJob(data: any) {
  if (!hasRedis()) return;
  await redisEvictConnection.rpush("index-insert-queue", JSON.stringify(data));
}

export async function processIndexInsertJobs() {
  if (!hasRedis() || !useIndex) return;
  const jobs =
    (await redisEvictConnection.lpop("index-insert-queue", 100)) ?? [];
  if (jobs.length === 0) return;
  try {
    const { error } = await index_supabase_service.from("index").insert(
      jobs.map(x => JSON.parse(x)),
    );
    if (error) _logger.error("Index inserter failed", { error });
  } catch (error) {
    _logger.error("Index inserter crashed", { error });
  }
}

export async function getIndexInsertQueueLength(): Promise<number> {
  if (!hasRedis()) return 0;
  return (await redisEvictConnection.llen("index-insert-queue")) ?? 0;
}

// -----------------------------
// RF Insert Jobs
// -----------------------------
export async function addIndexRFInsertJob(data: any) {
  if (!hasRedis()) return;
  await redisEvictConnection.rpush(
    "index-rf-insert-queue",
    JSON.stringify(data),
  );
}

export async function processIndexRFInsertJobs() {
  if (!hasRedis() || !useIndex) return;
  const jobs =
    (await redisEvictConnection.lpop("index-rf-insert-queue", 100)) ?? [];
  if (jobs.length === 0) return;
  try {
    const { error } = await index_supabase_service
      .from("request_frequency")
      .insert(jobs.map(x => JSON.parse(x)));
    if (error) _logger.error("Index RF inserter failed", { error });
  } catch (error) {
    _logger.error("Index RF inserter crashed", { error });
  }
}

// -----------------------------
// OMCE Jobs
// -----------------------------
export async function addOMCEJob(data: [number, string]) {
  if (!hasRedis()) return;
  await redisEvictConnection.sadd("omce-job-queue", JSON.stringify(data));
}

export async function processOMCEJobs() {
  if (!hasRedis() || !useIndex) return;
  const jobs =
    (await redisEvictConnection.spop("omce-job-queue", 100)) ?? [];
  if (jobs.length === 0) return;
  try {
    for (const job of jobs) {
      const [level, hash] = JSON.parse(job) as [number, string];
      const { error } = await index_supabase_service.rpc(
        "insert_omce_job_if_needed",
        { i_domain_level: level, i_domain_hash: hash },
      );
      if (error) _logger.error("OMCE job failed", { error });
    }
  } catch (error) {
    _logger.error("OMCE job crashed", { error });
  }
}

// -----------------------------
// Domain Frequency
// -----------------------------
export async function addDomainFrequencyJob(url: string) {
  if (!hasRedis()) return;
  try {
    const urlObj = new URL(url);
    let domain = urlObj.hostname;
    if (domain.startsWith("www.")) domain = domain.slice(4);
    await redisEvictConnection.rpush("domain-frequency-queue", domain);
  } catch (error) {
    _logger.warn("Failed to extract domain", { url, error });
  }
}

export async function processDomainFrequencyJobs() {
  if (!hasRedis() || !useIndex) return;
  const domains =
    (await redisEvictConnection.lpop("domain-frequency-queue", 100)) ?? [];
  if (domains.length === 0) return;
  const domainCounts = new Map<string, number>();
  for (const d of domains) {
    domainCounts.set(d, (domainCounts.get(d) || 0) + 1);
  }
  try {
    const updates = Array.from(domainCounts.entries()).map(([domain, count]) => ({
      domain,
      count,
    }));
    const { error } = await index_supabase_service.rpc(
      "upsert_domain_frequencies",
      { domain_updates: updates },
    );
    if (error) _logger.error("Domain frequency update failed", { error });
  } catch (error) {
    _logger.error("Domain frequency job crashed", { error });
  }
}

// -----------------------------
// Queries
// -----------------------------
export async function getTopDomains(limit = 100) {
  if (!useIndex) return [];
  const { data, error } = await index_supabase_service
    .from("domain_frequency")
    .select("domain, frequency, last_updated")
    .order("frequency", { ascending: false })
    .limit(limit);
  if (error) {
    _logger.error("Failed to get top domains", { error });
    return [];
  }
  return data ?? [];
}

export async function getDomainFrequency(domain: string) {
  if (!useIndex) return null;
  const { data, error } = await index_supabase_service
    .from("domain_frequency")
    .select("frequency")
    .eq("domain", domain)
    .single();
  if (error) return null;
  return data?.frequency ?? null;
}

export async function getDomainFrequencyStats() {
  if (!useIndex) return null;
  const { data, error } = await index_supabase_service
    .from("domain_frequency")
    .select("frequency");
  if (error || !data) return null;
  const totalRequests = data.reduce((sum, row) => sum + row.frequency, 0);
  return {
    totalDomains: data.length,
    totalRequests,
    avgRequestsPerDomain: Math.round(totalRequests / data.length),
  };
}
