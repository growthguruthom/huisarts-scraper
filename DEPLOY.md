# Deployment Guide — Huisarts Scraper

## Optie 1: GitHub Actions (GRATIS - aanbevolen)

### Setup

1. **Maak een GitHub repo** en push het project:
```bash
cd ~/Desktop/huisarts-scraper
git init
git add .
git commit -m "Initial commit"
gh repo create huisarts-scraper --private --push
```

2. **GitHub Secrets instellen** (Settings → Secrets → Actions):

| Secret | Waarde | Verplicht |
|--------|--------|-----------|
| `SMTP_USERNAME` | Je Gmail adres | Ja |
| `SMTP_PASSWORD` | Gmail App Password* | Ja |
| `NOTIFY_EMAIL` | Email voor notificaties (bijv. thom@growthguru.nl) | Ja |
| `GOOGLE_API_KEY` | Google CSE API key | Nee |
| `GOOGLE_CSE_ID` | Google CSE ID | Nee |

*Gmail App Password: Google Account → Security → 2FA aan → App Passwords → genereer voor "Mail"

3. **Dat is het!** De workflow draait automatisch elke dag om 08:00 CET.

### Hoe het werkt
```
Elke dag 08:00 CET (GitHub Actions cron)
    ↓
Checkout repo + restore SQLite DB
    ↓
Python scraper: bekendmakingen ophalen (laatste 2 dagen)
    ↓
Verrijken met adresgegevens + matchen met praktijken
    ↓
Export naar CSV + JSON
    ↓
Commit DB + exports naar repo
    ↓
Email als er nieuwe matches zijn
```

### Handmatig triggeren
GitHub repo → Actions → "Daily Huisarts Scraper" → Run workflow

### Kosten: **€0**

---

## Optie 2: VPS (self-hosted met Docker)

### Wat draait er?

```
VPS (bijv. Hetzner CX22 - €4/mo)
├── n8n           → workflow engine, dagelijkse trigger + email
├── scraper-api   → Python API die de scraper runt
└── scraper_data  → gedeeld volume met SQLite DB + exports
```

## Snelle setup

### 1. VPS aanmaken
- Hetzner CX22 (2 vCPU, 4GB RAM, 40GB SSD) — €4,51/mo
- Ubuntu 24.04
- SSH key toevoegen

### 2. Server inrichten
```bash
ssh root@jouw-server

# Docker installeren
curl -fsSL https://get.docker.com | sh
apt install -y docker-compose-plugin

# Project klonen of uploaden
mkdir -p /opt/huisarts-scraper
cd /opt/huisarts-scraper
# Upload bestanden via scp of git clone
```

### 3. Environment instellen
```bash
cp .env.example .env
nano .env
# Vul in: N8N_PASSWORD, SCRAPER_API_TOKEN
```

### 4. Starten
```bash
docker compose up -d
```

### 5. n8n configureren
1. Open `http://jouw-server:5678`
2. Log in met je N8N_USER/N8N_PASSWORD
3. **SMTP instellen:** Settings → Credentials → Add → SMTP
   - Gmail: `smtp.gmail.com`, port 587, TLS, App Password
4. **Workflow importeren:**
   - Workflows → Import from file → `n8n-workflow-daily.json`
5. **Email aanpassen:** In de Email nodes, pas `thom@growthguru.nl` aan
6. **Workflow activeren** (toggle rechtsboven)

## Workflow overzicht

```
Elke dag 08:00 (of handmatig)
    ↓
POST /run → scraper-api (start scraper)
    ↓
Wacht 90s
    ↓
GET /status → check of scraper klaar is
    ↓ (nog bezig? wacht 60s extra)
Parse resultaat
    ↓
Succesvol? ─── Ja → Nieuwe matches? ─── Ja → Email rapport
            │                         └── Nee → Log
            └── Nee → Email foutmelding
```

## Beheer

```bash
# Logs bekijken
docker compose logs -f scraper-api
docker compose logs -f n8n

# Handmatig scraper triggeren
curl -X POST http://localhost:8321/run \
  -H "Authorization: Bearer jouw-token"

# Status checken
curl http://localhost:8321/status \
  -H "Authorization: Bearer jouw-token"

# Database backup
docker compose cp scraper-api:/app/data/huisarts.db ./backup.db

# Updaten
docker compose down
docker compose build --no-cache
docker compose up -d
```

## HTTPS (optioneel maar aanbevolen)

Gebruik Caddy als reverse proxy:

```bash
apt install -y caddy
cat > /etc/caddy/Caddyfile << 'EOF'
n8n.jouw-domein.nl {
    reverse_proxy localhost:5678
}
EOF
systemctl restart caddy
```

Update dan `N8N_HOST=n8n.jouw-domein.nl` en `N8N_PROTOCOL=https` in `.env`.

## Kosten

| Component | Kosten |
|-----------|--------|
| Hetzner CX22 | €4,51/mo |
| Domein (optioneel) | ~€10/jaar |
| **Totaal** | **~€5/mo** |
