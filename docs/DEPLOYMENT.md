# Deployment Guide

This guide covers the easiest ways to host your Berkeley Ping Pong application.

## 🚀 Option 1: Render (Recommended - Easiest & Free)

**Render** is the easiest option with a free tier. It automatically detects your Flask app and deploys it.

### Steps:

1. **Push your code to GitHub** (if you haven't already)
   ```bash
   git add .
   git commit -m "Add deployment files"
   git push
   ```

2. **Sign up/Login to Render**
   - Go to https://render.com
   - Sign up with your GitHub account (free)

3. **Create a New Web Service**
   - Click "New +" → "Web Service"
   - Connect your GitHub repository
   - Select the `berkeleyPingPong` repository

4. **Configure the service:**
   - **Name**: `berkeley-ping-pong` (or any name you like)
   - **Region**: Choose closest to you
   - **Branch**: `main`
   - **Root Directory**: Leave empty
   - **Runtime**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn app:app`

5. **Add Environment Variables:**
   - Click "Environment" tab
   - Add:
     - `SUPABASE_URL` = your Supabase project URL
     - `SUPABASE_KEY` = your Supabase anon key
     - `PYTHON_VERSION` = `3.11.0` (optional)

6. **Deploy!**
   - Click "Create Web Service"
   - Wait 2-3 minutes for deployment
   - Your app will be live at `https://your-app-name.onrender.com`

**Free Tier Limits:**
- Spins down after 15 minutes of inactivity
- Takes ~30 seconds to wake up
- 750 hours/month free

---

## 🚂 Option 2: Railway (Also Very Easy)

**Railway** is another great option with a free tier.

### Steps:

1. **Sign up at Railway**
   - Go to https://railway.app
   - Sign up with GitHub

2. **Create New Project**
   - Click "New Project"
   - Select "Deploy from GitHub repo"
   - Choose your `berkeleyPingPong` repository

3. **Configure:**
   - Railway auto-detects Flask apps
   - Add environment variables:
     - `SUPABASE_URL`
     - `SUPABASE_KEY`
   - Set start command: `gunicorn app:app`

4. **Deploy**
   - Railway will automatically deploy
   - Get your URL: `https://your-app.up.railway.app`

**Free Tier:**
- $5 credit/month
- No spin-down (always on)

---

## ✈️ Option 3: Fly.io (Good Free Tier)

**Fly.io** offers a generous free tier.

### Steps:

1. **Install Fly CLI:**
   ```bash
   curl -L https://fly.io/install.sh | sh
   ```

2. **Login:**
   ```bash
   fly auth login
   ```

3. **Create app:**
   ```bash
   fly launch
   ```
   - Follow prompts
   - Don't deploy yet

4. **Set secrets:**
   ```bash
   fly secrets set SUPABASE_URL=your_url
   fly secrets set SUPABASE_KEY=your_key
   ```

5. **Deploy:**
   ```bash
   fly deploy
   ```

**Free Tier:**
- 3 shared-cpu VMs
- 3GB persistent volumes
- 160GB outbound data transfer

---

## 🔧 Production Configuration

All options use `gunicorn` as the production server. The app is configured to:
- Use environment variables for configuration
- Run on the port specified by the hosting platform
- Work with or without debug mode

### Environment Variables Needed:
- `SUPABASE_URL` - Your Supabase project URL
- `SUPABASE_KEY` - Your Supabase anon/service key

---

## 📝 Quick Comparison

| Platform | Ease | Free Tier | Always On | Best For |
|----------|------|-----------|-----------|----------|
| **Render** | ⭐⭐⭐⭐⭐ | ✅ | ❌ (spins down) | Getting started quickly |
| **Railway** | ⭐⭐⭐⭐ | ✅ ($5 credit) | ✅ | Always-on free option |
| **Fly.io** | ⭐⭐⭐ | ✅ | ✅ | More control, CLI-based |

**Recommendation:** Start with **Render** for the easiest setup, then move to Railway if you need always-on.

---

## 🐛 Troubleshooting

### App won't start
- Check that `SUPABASE_URL` and `SUPABASE_KEY` are set correctly
- Check build logs for dependency issues
- Ensure `gunicorn` is in `requirements.txt`

### Database connection errors
- Verify your Supabase credentials
- Check Supabase project is active
- Ensure network access is allowed

### Slow first load (Render)
- This is normal on free tier (cold start)
- Consider upgrading to paid tier for faster starts

