# 🚀 Quick Deploy Guide

**Stack reminder:** **Supabase** (DB, free) + **Render** (app, free) + **GitHub Actions** weekly import/scrape + optional **FastCron** pings to ease Render cold starts. Details: [INFRASTRUCTURE.md](./INFRASTRUCTURE.md).

## Fastest Option: Render (5 minutes)

1. **Push to GitHub** (if not already done)
   ```bash
   git add .
   git commit -m "Ready for deployment"
   git push
   ```

2. **Go to Render.com**
   - Sign up/login at https://render.com
   - Click "New +" → "Web Service"
   - Connect your GitHub repo

3. **Configure:**
   - Name: `berkeley-ping-pong`
   - Build: `pip install -r requirements.txt`
   - Start: `gunicorn app:app`

4. **Add Secrets:**
   - `SUPABASE_URL` = (your Supabase URL)
   - `SUPABASE_KEY` = (your Supabase key)

5. **Deploy!** 
   - Click "Create Web Service"
   - Wait 2-3 minutes
   - Done! 🎉

Your app will be at: `https://your-app-name.onrender.com`

---

**Note:** Free tier spins down after 15 min inactivity (takes ~30 sec to wake up)

For always-on free hosting, use **Railway** (see DEPLOYMENT.md)

