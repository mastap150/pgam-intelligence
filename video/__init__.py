"""
Destination.com Video Engine (DVE).

End-to-end automated video content system: content intelligence → concepts →
hooks → scripts → assets → rendered branded vertical video → QA → human
approval → YouTube → analytics → learning.

Architecture: docs/destination-video-architecture.md
Schema:       migrations/2026_09_05_destination_video_engine.sql
Entry points: video/pipeline.py (MVP loop), video/dashboard.py (approval UI),
              scheduler.py jobs behind PGAM_DVE_ENABLED=1.
"""
