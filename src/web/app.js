/**
 * DealHunter Monitor - App Logic
 * Fetches data from the local API to display timers and queue.
 */

const API_Base = '/api';
const REFRESH_INTERVAL_QUEUE = 30000; // 30s
let nextScrapeTime = null;
let nextSendTime = null;

// DOM Elements
const els = {
    statusDot: document.getElementById('system-status-dot'),
    statusText: document.getElementById('system-status-text'),
    timerScrape: document.getElementById('timer-scraper'),
    timeScrape: document.getElementById('time-scraper'),
    timerSend: document.getElementById('timer-sender'),
    timeSend: document.getElementById('time-sender'),
    senderIcon: document.getElementById('sender-icon'),
    senderStatusBadge: document.getElementById('sender-status-badge'),
    
    queueCount: document.getElementById('queue-count'),
    queueListCount: document.getElementById('queue-list-count'),
    queueList: document.getElementById('queue-list'),
    queueLoader: document.getElementById('queue-loader'),
    queueEmpty: document.getElementById('queue-empty'),
    btnRefresh: document.getElementById('btn-refresh'),
    template: document.getElementById('queue-item-template')
};

/**
 * Helper to Format Time HH:MM:SS
 */
function formatTime(date) {
    if (!date) return "--:--:--";
    return date.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

/**
 * Format Currency BRL
 */
function formatCurrency(val) {
    if (!val && val !== 0) return "R$ 0,00";
    return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(val);
}

/**
 * Calculate distance and format HH:MM:SS
 */
function getTimerString(targetDate) {
    if (!targetDate) return "--:--:--";
    const now = new Date();
    const diff = targetDate - now;
    
    if (diff <= 0) return "Processando...";
    
    const h = Math.floor(diff / (1000 * 60 * 60));
    const m = Math.floor((diff / 1000 / 60) % 60);
    const s = Math.floor((diff / 1000) % 60);
    
    return `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
}

/**
 * Fetch System State (Timers)
 */
async function fetchState() {
    try {
        const res = await fetch(`${API_Base}/state`);
        if (!res.ok) throw new Error('API Error');
        const data = await res.json();
        
        // Update connection status
        els.statusDot.className = 'status-dot online';
        els.statusText.textContent = 'Sistema Online';
        
        // Update variables
        nextScrapeTime = data.next_scrape_time ? new Date(data.next_scrape_time) : null;
        nextSendTime = data.next_send_time ? new Date(data.next_send_time) : null;
        
        // Update static UI
        els.timeScrape.textContent = nextScrapeTime ? `Próximo às ${formatTime(nextScrapeTime)}` : "Aguardando evento...";
        els.timeSend.textContent = nextSendTime ? `Próximo às ${formatTime(nextSendTime)}` : "Aguardando evento...";
        
        // Sender Status Badge
        if (data.is_sending_hours) {
            els.senderStatusBadge.className = 'badge bg-green';
            els.senderStatusBadge.textContent = 'Ativo';
            els.senderIcon.style.opacity = '1';
        } else {
            els.senderStatusBadge.className = 'badge bg-gray';
            els.senderStatusBadge.textContent = 'Fora de Horário';
            els.senderIcon.style.opacity = '0.5';
        }
    } catch (err) {
        console.error("State Fetch Error:", err);
        els.statusDot.className = 'status-dot offline';
        els.statusText.textContent = 'Sistema Offline (Tentando reconectar)';
    }
}

/**
 * Loop the countdown display
 */
function startTimersLoop() {
    setInterval(() => {
        els.timerScrape.textContent = getTimerString(nextScrapeTime);
        els.timerSend.textContent = getTimerString(nextSendTime);
    }, 1000);
}

/**
 * Color based on score value
 */
function getScoreColor(score) {
    if (score >= 90) return "#10b981"; // Green
    if (score >= 75) return "#f59e0b"; // Amber
    if (score >= 60) return "#fcd34d"; // Yellow
    return "#ef4444"; // Red
}

/**
 * Fetch and Render Queue List
 */
async function fetchQueue() {
    try {
        const res = await fetch(`${API_Base}/queue`);
        if (!res.ok) throw new Error('API Error');
        const data = await res.json();
        
        renderQueue(data.queue || []);
        
    } catch (err) {
        console.error("Queue Fetch Error:", err);
        // Fallback UI or silent error
    }
}

function renderQueue(offers) {
    // Hide Loader
    els.queueLoader.style.display = 'none';
    
    // Update Counts
    els.queueCount.textContent = offers.length;
    els.queueListCount.textContent = offers.length;
    
    if (offers.length === 0) {
        els.queueList.style.display = 'none';
        els.queueEmpty.style.display = 'flex';
        return;
    }
    
    els.queueEmpty.style.display = 'none';
    els.queueList.style.display = 'grid';
    els.queueList.innerHTML = ''; // Clear previous
    
    offers.forEach((offer, index) => {
        const clone = els.template.content.cloneNode(true);
        const itemEl = clone.querySelector('.queue-item');
        
        // Ranking
        itemEl.querySelector('.ranking-number').textContent = `#${index + 1}`;
        
        // Thumbnail & Discount
        const img = itemEl.querySelector('img');
        img.src = offer.thumbnail_url || 'https://via.placeholder.com/100?text=No+Image';
        itemEl.querySelector('.discount-badge').textContent = `-${offer.discount_percent}%`;
        
        // Details
        itemEl.querySelector('.item-title').textContent = offer.title || "Unknown Product";
        
        const catBadge = itemEl.querySelector('.meta-category');
        if (offer.category) {
            catBadge.textContent = offer.category;
        } else {
            catBadge.style.display = 'none';
        }
        
        const badgeBadge = itemEl.querySelector('.meta-badge');
        if (offer.badge) {
            badgeBadge.textContent = offer.badge;
            badgeBadge.style.display = 'inline-block';
        }
        
        const shipBadge = itemEl.querySelector('.meta-shipping');
        if (offer.free_shipping) {
            shipBadge.style.display = 'inline-block';
        }
        
        // Prices
        itemEl.querySelector('.price-current').textContent = formatCurrency(offer.current_price);
        itemEl.querySelector('.price-original').textContent = formatCurrency(offer.original_price);
        
        // Score Circle
        const score = offer.final_score || 0;
        itemEl.querySelector('.score-value').textContent = score;
        const circle = itemEl.querySelector('.circle');
        circle.setAttribute('stroke-dasharray', `${score}, 100`);
        circle.setAttribute('stroke', getScoreColor(score));
        
        // Actions
        const btn = itemEl.querySelector('.action-link');
        if (offer.product_url) {
            btn.href = offer.product_url;
        } else {
            btn.style.display = 'none';
        }
        
        els.queueList.appendChild(clone);
    });
}

/**
 * Initialization
 */
function init() {
    // Initial fetches
    fetchState();
    fetchQueue();
    
    // Set intervals
    setInterval(fetchState, 5000); // Poll state every 5s
    setInterval(fetchQueue, REFRESH_INTERVAL_QUEUE); // Poll queue
    
    startTimersLoop();
    
    // Bind Events
    els.btnRefresh.addEventListener('click', () => {
        els.btnRefresh.style.opacity = '0.5';
        fetchQueue().then(() => {
            els.btnRefresh.style.opacity = '1';
        });
    });
}

// Boot
document.addEventListener('DOMContentLoaded', init);
