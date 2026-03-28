/** Tipos espelhando o schema do Supabase */

export interface Product {
  id: string;
  ml_id: string;
  title: string;
  current_price: number;
  original_price: number | null;
  pix_price: number | null;
  discount_percent: number;
  rating_stars: number | null;
  rating_count: number | null;
  free_shipping: boolean;
  installments_without_interest: boolean;
  thumbnail_url: string | null;
  product_url: string;
  category_id: string | null;
  badge_id: string | null;
  brand_id: string | null;
  first_seen_at: string;
  last_seen_at: string;
  deleted_at: string | null;
}

export interface ScoredOffer {
  id: string;
  product_id: string;
  rule_score: number;
  final_score: number;
  status: "pending" | "approved" | "rejected";
  scored_at: string;
  queue_priority: number;
  score_override: number | null;
  admin_notes: string | null;
}

export interface SentOffer {
  id: string;
  scored_offer_id: string;
  user_id: string | null;
  channel: "telegram" | "whatsapp";
  sent_at: string;
  triggered_by: "auto" | "admin";
}

/** Linha da view vw_approved_unsent */
export interface QueueItem {
  product_id: string;
  ml_id: string;
  title: string;
  current_price: number;
  original_price: number | null;
  pix_price: number | null;
  discount_percent: number;
  free_shipping: boolean;
  thumbnail_url: string | null;
  product_url: string;
  rating_stars: number | null;
  rating_count: number | null;
  installments_without_interest: boolean;
  brand: string | null;
  category: string | null;
  badge: string | null;
  scored_offer_id: string;
  final_score: number;
  scored_at: string;
  queue_priority: number;
  score_override: number | null;
  admin_notes: string | null;
}

/** Linha da view vw_top_deals */
export interface TopDeal {
  product_id: string;
  ml_id: string;
  title: string;
  current_price: number;
  original_price: number | null;
  pix_price: number | null;
  discount_percent: number;
  free_shipping: boolean;
  thumbnail_url: string | null;
  product_url: string;
  brand: string | null;
  category: string | null;
  badge: string | null;
  final_score: number;
}

/** Linha da materialized view mv_last_24h_summary */
export interface DailySummary {
  products_scraped: number;
  offers_scored: number;
  offers_approved: number;
  offers_sent: number;
  avg_score: number | null;
  max_discount_pct: number | null;
}

/** Score distribution histogram bucket */
export interface ScoreBucket {
  score_bucket: number;
  count: number;
}

/** Hourly sends */
export interface HourlySend {
  hour: number;
  count: number;
}

/** Daily metrics for trend charts */
export interface DailyMetric {
  day: string;
  products_scraped: number;
  offers_scored: number;
  offers_approved: number;
  offers_sent: number;
  avg_score: number | null;
  avg_discount: number | null;
}

/** Conversion funnel */
export interface ConversionFunnel {
  scraped: number;
  scored: number;
  approved: number;
  sent: number;
}

/** Offer row for the offers table (joined product + scored_offer) */
export interface OfferRow {
  // From products
  product_id: string;
  ml_id: string;
  title: string;
  current_price: number;
  original_price: number | null;
  discount_percent: number;
  thumbnail_url: string | null;
  product_url: string;
  free_shipping: boolean;
  // From scored_offers
  scored_offer_id: string;
  final_score: number;
  status: string;
  scored_at: string;
  queue_priority: number;
  admin_notes: string | null;
  // From joins
  brand: string | null;
  category: string | null;
  badge: string | null;
}
