"use client";

import React, { useEffect, useState } from "react";
import { useSupabase } from "@/hooks/use-supabase";
import { useAdminApi } from "@/hooks/use-admin-api";
import {
  Globe,
  Calculator,
  CheckCircle,
  Send,
  RefreshCw,
  Play,
  FastForward,
  Clock,
  Activity,
  ChevronDown,
} from "lucide-react";
import { formatCurrency, formatNumber, cn } from "@/lib/utils";
import type { DailySummary, TopDeal } from "@/lib/types";
import { toast } from "sonner";

// --- Custom Hooks ---
function useCountUp(endValue: number, durationMs = 1500) {
  const [value, setValue] = useState(0);

  useEffect(() => {
    let startTimestamp: number | null = null;
    let animFrame: number;

    const step = (timestamp: number) => {
      if (!startTimestamp) startTimestamp = timestamp;
      const progress = Math.min((timestamp - startTimestamp) / durationMs, 1);
      const ease = progress === 1 ? 1 : 1 - Math.pow(2, -10 * progress);
      setValue(Number((ease * endValue).toFixed(endValue % 1 !== 0 ? 1 : 0)));
      if (progress < 1) animFrame = window.requestAnimationFrame(step);
    };
    animFrame = window.requestAnimationFrame(step);

    return () => {
      if (animFrame) window.cancelAnimationFrame(animFrame);
    };
  }, [endValue, durationMs]);

  return value;
}

// --- UI Components ---
interface FunnelKpiProps {
  title: string;
  value: number;
  trend: string | null;
  icon: React.ElementType;
  colorClass: string;
  shadowClass: string;
  hasConnector: boolean;
}

function FunnelKpiCard({ title, value, trend, icon: Icon, colorClass, shadowClass, hasConnector }: FunnelKpiProps) {
  const animatedValue = useCountUp(value);
  const isPositive = trend && !trend.startsWith("-");

  return (
    <div className="relative flex-1 group">
      <div
        className={cn(
          "bg-card border border-border rounded-lg p-5 relative overflow-hidden transition-all duration-300 hover:-translate-y-1 shadow-sm",
          shadowClass
        )}
      >
        <div className="flex justify-between items-start mb-4">
          <div className={cn("p-2 rounded-md bg-secondary ring-1 ring-border", colorClass)}>
            <Icon size={20} />
          </div>
          {trend && (
            <span
              className={cn(
                "text-[11px] font-bold px-2 py-1 rounded-md bg-secondary",
                isPositive ? "text-[#2ECC71]" : "text-[#E24B4A]"
              )}
            >
              {trend}
            </span>
          )}
        </div>
        <p className="text-[13px] text-muted-foreground font-medium mb-1">{title}</p>
        <p className="text-[24px] font-display font-medium text-foreground tracking-tight tabular-nums">{animatedValue}</p>
      </div>
      {hasConnector && (
        <div className="hidden xl:block absolute top-1/2 -right-2 w-2 border-t-2 border-dashed border-primary/30 -translate-y-1/2 z-0" />
      )}
    </div>
  );
}

function SecondaryKpi({ title, value, suffix, trend }: { title: string; value: number; suffix: string; trend: string | null }) {
  const animValue = useCountUp(value);
  const isPositive = trend && !trend.startsWith("-");

  return (
    <div className="bg-card border border-border rounded-lg p-5 transition duration-300 relative group overflow-hidden shadow-sm">
      <div className="absolute top-0 left-0 w-full h-0.5 bg-gradient-to-r from-transparent via-primary/50 to-transparent opacity-0 group-hover:opacity-100 transition-opacity" />
      <p className="text-[13px] text-muted-foreground font-medium mb-2">{title}</p>
      <div className="flex items-end gap-2">
        <p className="text-[24px] font-display font-medium text-foreground tracking-tight tabular-nums">
          {animValue}
          {suffix}
        </p>
        {trend && (
          <p
            className={cn(
              "text-xs font-medium mb-1",
              isPositive ? "text-[#2ECC71]" : "text-[#E24B4A]"
            )}
          >
            {trend}
          </p>
        )}
      </div>
    </div>
  );
}

function ScoreBadge({ score }: { score: number }) {
  const getStyleClasses = (s: number) => {
    if (s >= 90) return "bg-gradient-to-br from-primary to-accent text-white font-extrabold";
    if (s >= 70) return "bg-success text-white";
    if (s >= 40) return "bg-warning text-white";
    return "bg-destructive text-white";
  };
  return (
    <div className={cn("w-11 h-11 rounded-full flex items-center justify-center font-sans text-sm font-bold shrink-0", getStyleClasses(score))}>
      {Math.round(score)}
    </div>
  );
}

function SystemAction({ icon: Icon, label, colorClass, onClick, loading }: any) {
  return (
    <button
      onClick={onClick}
      disabled={loading}
      className="w-full flex items-center gap-3 p-3 rounded-xl border border-border bg-secondary hover:bg-secondary hover:bg-secondary/80 transition disabled:opacity-50 disabled:cursor-not-allowed group"
    >
      <div className={cn("p-1.5 rounded-lg bg-background shadow-sm dark:shadow-none dark:bg-black/30 dark:group-hover:bg-black/50 transition", colorClass)}>
        {loading ? <RefreshCw className="animate-spin" size={16} /> : <Icon size={16} />}
      </div>
      <span className="text-sm text-foreground font-medium">{label}</span>
    </button>
  );
}

export default function DashboardPage() {
  const supabase = useSupabase();
  const api = useAdminApi();
  const [summary, setSummary] = useState<DailySummary | null>(null);
  const [topDeals, setTopDeals] = useState<TopDeal[]>([]);
  const [systemState, setSystemState] = useState<Record<string, unknown> | null>(null);
  
  const [loadingScrape, setLoadingScrape] = useState(false);
  const [loadingSend, setLoadingSend] = useState(false);
  const [loadingRefresh, setLoadingRefresh] = useState(false);

  async function fetchData() {
    setLoadingRefresh(true);
    try {
      // Summary from view
      const { data: summaryData } = await supabase
        .from("mv_last_24h_summary")
        .select("*")
        .single();
      if (summaryData) setSummary(summaryData);

      // Top deals
      const { data: deals } = await supabase
        .from("vw_top_deals")
        .select("*")
        .limit(5);
      if (deals) setTopDeals(deals);

      // System state from FastAPI
      try {
        const stateRes = await fetch(
          `${process.env.NEXT_PUBLIC_FASTAPI_URL || "http://localhost:8000"}/api/state`
        );
        if (stateRes.ok) setSystemState(await stateRes.json());
      } catch {
        // Keep previous state if failing silently
      }
    } catch (err) {
      console.error("Dashboard fetch error:", err);
      toast.error("Erro ao sincronizar dashboard");
    } finally {
      setLoadingRefresh(false);
    }
  }

  useEffect(() => {
    fetchData();

    // Realtime: system_logs
    const channel = supabase
      .channel("dashboard-realtime")
      .on(
        "postgres_changes",
        { event: "INSERT", schema: "public", table: "sent_offers" },
        () => fetchData()
      )
      .subscribe();

    return () => {
      supabase.removeChannel(channel);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleForceScraping = async () => {
    setLoadingScrape(true);
    try {
      await api.scrapeNow();
      toast.success("Scraping forçado iniciado");
      await fetchData();
    } catch (err) {
      toast.error(`Falha no scaping: ${err instanceof Error ? err.message : "Erro!"}`);
    } finally {
      setLoadingScrape(false);
    }
  };

  const handleSendNow = async () => {
    setLoadingSend(true);
    try {
      await api.sendNow();
      toast.success("Comando enviado com sucesso");
      await fetchData();
    } catch (err) {
      toast.error(`Falha no envio: ${err instanceof Error ? err.message : "Erro!"}`);
    } finally {
      setLoadingSend(false);
    }
  };

  return (
    <div className="min-h-screen text-foreground font-sans selection:bg-success/30 selection:text-foreground pb-10 animate-in fade-in duration-500">
      
      {/* HEADER */}
      <header className="flex flex-col md:flex-row justify-between items-start md:items-end mb-8 gap-4">
        <div>
          <h2 className="text-3xl font-bold text-foreground tracking-tight">Visão geral</h2>
          <p className="text-muted-foreground mt-1">Monitoramento em tempo real</p>
        </div>
        <div className="flex items-center gap-2 bg-secondary border border-border px-4 py-2 rounded-xl text-sm shadow-sm backdrop-blur-md">
          <Clock size={16} className="text-muted-foreground" />
          <span className="font-mono text-foreground ml-1">
            Atualizado {new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })}
          </span>
          <div className={cn("w-1.5 h-1.5 rounded-full ml-2", loadingRefresh ? "bg-amber-500" : "bg-success animate-pulse")} />
        </div>
      </header>

      {/* PIPELINE DE KPIS */}
      <div className="flex flex-col xl:flex-row gap-2 xl:gap-4 mb-4">
        <FunnelKpiCard
          title="Scrapeados"
          value={summary?.products_scraped || 0}
          trend={null}
          icon={Globe}
          colorClass="text-primary bg-primary/10"
          shadowClass="border-primary/15"
          hasConnector={true}
        />
        <FunnelKpiCard
          title="Pontuadas"
          value={summary?.offers_scored || 0}
          trend={null}
          icon={Calculator}
          colorClass="text-accent bg-accent/10"
          shadowClass="border-accent/15"
          hasConnector={true}
        />
        <FunnelKpiCard
          title="Aprovadas"
          value={summary?.offers_approved || 0}
          trend={null}
          icon={CheckCircle}
          colorClass="text-success bg-success/10"
          shadowClass="border-success/15"
          hasConnector={true}
        />
        <FunnelKpiCard
          title="Enviadas"
          value={summary?.offers_sent || 0}
          trend={null}
          icon={Send}
          colorClass="text-primary bg-primary/10"
          shadowClass="border-primary/15"
          hasConnector={false}
        />
      </div>

      {/* SECUNDARY KPIS */}
      <div className="grid grid-cols-2 lg:grid-cols-3 gap-2 xl:gap-4 mb-8">
        <SecondaryKpi
          title="Score Médio (24h)"
          value={summary?.avg_score ? Number(summary.avg_score.toFixed(1)) : 0}
          suffix=""
          trend={null}
        />
        <SecondaryKpi
          title="Maior Desconto Hoje"
          value={summary?.max_discount_pct ? Number(summary.max_discount_pct.toFixed(1)) : 0}
          suffix="%"
          trend={null}
        />
        <SecondaryKpi
          title="Taxa de Aprovação"
          value={summary?.products_scraped ? Number(((summary.offers_approved / summary.products_scraped) * 100).toFixed(1)) : 0}
          suffix="%"
          trend={null}
        />
      </div>

      {/* CONTEÚDO INFERIOR */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 xl:gap-8">
        
        {/* Top Deals List */}
        <div className="lg:col-span-2 space-y-4">
          <div className="flex justify-between items-center mb-2 px-1">
            <h3 className="text-xl font-bold flex items-center gap-2">
              Top Deals
            </h3>
            <button className="text-xs text-muted-foreground hover:text-foreground flex items-center gap-1 transition-colors">
              Últimas 6 horas <ChevronDown size={14} />
            </button>
          </div>

          <div className="bg-card border border-border rounded-2xl overflow-hidden shadow-lg">
            {topDeals.map((deal, idx) => (
              <div
                key={deal.product_id}
                className="group flex flex-col sm:flex-row items-start sm:items-center gap-4 p-4 lg:p-5 border-b border-border last:border-0 hover:bg-secondary transition duration-300 cursor-pointer"
              >
                <span className="font-mono font-bold text-muted-foreground group-hover:text-muted-foreground w-8 tabular-nums text-center">
                  {idx === 0 ? <span className="text-2xl">🥇</span> : idx === 1 ? <span className="text-2xl">🥈</span> : idx === 2 ? <span className="text-2xl">🥉</span> : <span className="text-sm">{idx + 1}</span>}
                </span>
                
                <div className="w-14 h-14 rounded-xl bg-secondary border border-border shrink-0 overflow-hidden relative flex items-center justify-center">
                   {deal.thumbnail_url ? (
                     <img src={deal.thumbnail_url} alt="" className="w-full h-full object-cover mix-blend-multiply dark:mix-blend-screen opacity-90" />
                   ) : (
                     <div className="w-full h-full bg-muted" />
                   )}
                </div>
                
                <div className="flex-1 min-w-0">
                  <h4 className="font-medium text-foreground truncate pr-4 text-[15px]">{deal.title}</h4>
                  <div className="flex flex-wrap items-center gap-2 mt-1.5">
                    {deal.category && (
                      <span className="px-2 py-0.5 rounded bg-muted border border-border text-[10px] uppercase tracking-wider text-muted-foreground">
                        {deal.category}
                      </span>
                    )}
                    <span className="px-2 py-0.5 rounded bg-success/10 text-success text-[10px] uppercase tracking-wider font-bold">
                      -{Math.round(deal.discount_percent)}% OFF
                    </span>
                  </div>
                </div>

                <div className="text-left sm:text-right shrink-0 mt-2 sm:mt-0 w-full sm:w-28">
                  <p className="font-mono text-lg font-bold text-foreground tracking-tight tabular-nums">
                    {formatCurrency(deal.current_price)}
                  </p>
                  {deal.original_price && (
                    <p className="font-mono text-[11px] text-muted-foreground line-through opacity-70 decoration-gray-600 tabular-nums">
                      {formatCurrency(deal.original_price)}
                    </p>
                  )}
                </div>

                <div className="shrink-0 hidden sm:block pl-2 ml-2 border-l border-border">
                  <ScoreBadge score={deal.final_score} />
                </div>
              </div>
            ))}
            
            {topDeals.length === 0 && !loadingRefresh && (
              <div className="p-10 text-center">
                <p className="text-muted-foreground text-sm">Nenhum deal excelente nas últimas horas.</p>
              </div>
            )}
          </div>
        </div>

        {/* System Control Sidebar */}
        <div className="space-y-6">
          
          {/* Status Panel */}
          <div className="bg-card border border-border rounded-2xl p-6 relative overflow-hidden">
            <div className="flex items-center justify-between mb-6 pb-4 border-b border-border">
              <h3 className="font-bold text-lg text-foreground">Status do Bot</h3>
              <div className={cn("flex items-center gap-2 px-3 py-1 rounded-full border", systemState ? "bg-success/15 border-success/40" : "bg-destructive/15 border-destructive/40")}>
                <span className="relative flex h-2.5 w-2.5">
                  {systemState && <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-success opacity-60" />}
                  <span className={cn("relative inline-flex rounded-full h-2.5 w-2.5", systemState ? "bg-success" : "bg-destructive")} />
                </span>
                <span className={cn("text-xs font-bold uppercase tracking-wider", systemState ? "text-success" : "text-destructive")}>
                  {systemState ? "Online" : "Offline"}
                </span>
              </div>
            </div>

            <div className="space-y-6">
              <div>
                <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1 font-medium">
                  <Activity size={14} className="text-blue-400" />
                  Robô de Scraping
                </div>
                <div className="flex justify-between items-end mt-2">
                  <p className="text-foreground font-medium text-sm">Próximo</p>
                  <p className="text-xs text-muted-foreground font-mono">
                    {systemState?.next_scrape_time ? String(systemState.next_scrape_time).slice(11, 19) : "--:--:--"}
                  </p>
                </div>
                <div className="w-full bg-secondary h-1 rounded-full mt-2 overflow-hidden shadow-inner">
                  <div className="bg-primary h-full w-[100%] rounded-full opacity-50 animate-[pulse_3s_ease-in-out_infinite]" />
                </div>
              </div>

              <div>
                <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1 font-medium">
                  <Send size={14} className="text-sky-400" />
                  Robô de Envio
                </div>
                <div className="flex justify-between items-end mt-2">
                  <p className="text-foreground font-medium text-sm">Próximo</p>
                  <p className="text-xs text-muted-foreground font-mono">
                    {systemState?.next_send_time ? String(systemState.next_send_time).slice(11, 19) : "--:--:--"}
                  </p>
                </div>
                <div className="w-full bg-secondary h-1 rounded-full mt-2 overflow-hidden shadow-inner">
                   <div className="bg-accent h-full w-[100%] rounded-full opacity-50 animate-[pulse_4s_ease-in-out_infinite]" />
                </div>
              </div>
            </div>
          </div>

          {/* Quick Actions */}
          <div className="bg-card border border-border rounded-2xl p-6">
             <h3 className="font-bold mb-4 text-foreground">Ações Rápidas</h3>
             <div className="space-y-3">
               <SystemAction 
                 icon={Play} 
                 label="Forçar Scraping Imediato" 
                 colorClass="text-accent"
                 loading={loadingScrape}
                 onClick={handleForceScraping}
               />
               <SystemAction 
                 icon={FastForward} 
                 label="Acelerar Envio da Fila" 
                 colorClass="text-primary"
                 loading={loadingSend}
                 onClick={handleSendNow}
               />
               <SystemAction 
                 icon={RefreshCw} 
                 label="Sincronizar Dashboard" 
                 colorClass="text-muted-foreground"
                 loading={loadingRefresh}
                 onClick={() => fetchData()}
               />
             </div>
          </div>

        </div>
      </div>

    </div>
  );
}
