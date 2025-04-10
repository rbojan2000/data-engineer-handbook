# Spotify – User Journey & Experiment Design

## User Journey

I started using Spotify in 2016, looking for a better music streaming experience—ad-free and high-quality. Right away, I was impressed by the **high audio quality (320 kbps)**, which made a noticeable difference compared to other platforms I had tried.

One of the most powerful features early on was the **ability to create and organize playlists**. I could easily tailor my listening experience to my mood or activity. Then I discovered **Discover Weekly**, which completely changed how I found music. Spotify’s **recommendation algorithm** is still one of the most impressive parts of the product—it consistently suggests new artists I actually enjoy.

Another big plus is the app's **minimalist and clean design**. It doesn’t get in the way—it lets me focus on the music. Over time, I’ve come to use Spotify in almost every context of my daily life: while working out, commuting, relaxing, or even sleeping.

Some of the more recent features that I’ve grown to love are **Friends Activity**, where I can see what people I follow are listening to in real time, and **Group Sessions**, which make it easy to listen to the same music with friends remotely. These social layers have added emotional depth and fun to the platform. Spotify is no longer just a music app to me—it’s part of my daily rhythm.

---

## Experiment 1: Karaoke Mode

### Objective:
Test whether adding a karaoke mode increases user engagement and makes music consumption more interactive and social.

### Background:
Many users turn to external apps for karaoke-style singing (e.g., Smule). Spotify could introduce its own integrated karaoke mode where users can sing along to instrumental versions of tracks, follow live lyrics, and optionally record or share their performances. This would appeal especially to younger users and those listening with friends.

### Null Hypothesis (H₀):
The karaoke feature will have no significant effect on time spent in the app or interaction with music.

### Alternative Hypothesis (H₁):
The karaoke mode will increase daily engagement, session duration, and social sharing within the app.

### Leading Metrics:
- Average time spent in-app per user per day  
- Number of karaoke sessions started per user

### Lagging Metrics:
- 30-day user retention  
- Number of tracks/shared playlists  
- Number of friend invites through the feature

### Test Cell Allocation:

| Test Cell | Description                                    | % of Users |
|-----------|------------------------------------------------|------------|
| Control   | No karaoke feature                             | 33%        |
| Test A    | Karaoke mode automatically enabled             | 33%        |
| Test B    | Karaoke mode available as opt-in (toggle)      | 33%        |

---

## Experiment 2: Contextual Play Button

### Objective:
Test whether a dynamic "Play" button that adapts based on user context increases session frequency and listening duration.

### Background:
Currently, pressing “Play” typically resumes the last played song or playlist. In this experiment, the button would instead use contextual signals (time of day, device type, past behavior) to automatically play content that fits the moment. For example:
- Morning = “Wake up mix”
- Driving = “Driving mode”
- Late night = “Chill & Relax”
- Weekend = “Weekend vibes”

The idea is to reduce friction by helping users get to relevant content faster, without needing to browse or search.

### Null Hypothesis (H₀):
The contextual Play button has no effect on session count or total listening time.

### Alternative Hypothesis (H₁):
The contextual Play button increases daily sessions and listening time by offering content that better matches the user’s intent.

### Leading Metrics:
- Clicks on contextual Play per user  
- Number of sessions per day  
- Average session duration

### Lagging Metrics:
- 7-day and 30-day user retention  
- Free-to-Premium conversion rate  
- Net Promoter Score (NPS)

### Test Cell Allocation:

| Test Cell | Description                                                        | % of Users |
|-----------|--------------------------------------------------------------------|------------|
| Control   | Standard Play button (resumes last played content)                | 50%        |
| Test      | Dynamic Play button based on user’s context and time of day       | 50%        |

---

## Experiment 3: Daily Audio Journal

### Objective:
Explore whether a personal, emotional feature like audio journaling increases user retention and emotional attachment to the app.

### Background:
With rising interest in mindfulness and self-reflection, Spotify could allow users to record short daily audio entries—like a voice diary. These would be private by default, but optional to share. The idea is to create a personal, emotional ritual within the app, turning Spotify into more than just a music player.

Spotify could prompt users with optional questions like:
- “How are you feeling today?”
- “What song would you dedicate to yourself today?”

This could lead to deeper daily engagement and give Spotify richer insight into mood-based music preferences.

### Null Hypothesis (H₀):
The audio journal feature has no significant impact on user activity or long-term retention.

### Alternative Hypothesis (H₁):
Users with access to the audio journal return more frequently and feel more emotionally connected to the app.

### Leading Metrics:
- Daily Journal usage rate  
- Number of recordings per user per week  
- Average length of journal entries

### Lagging Metrics:
- 7-day and 30-day retention  
- Net Promoter Score (NPS)  
- Time spent in app per week

### Test Cell Allocation:

| Test Cell | Description                              | % of Users |
|-----------|------------------------------------------|------------|
| Control   | No access to Daily Journal               | 50%        |
| Test      | Full access to record daily audio logs   | 50%        |

---
