#!/bin/bash

echo "=== Pipeline Status Check ==="
echo ""

echo "🔍 Browser Pipeline:"
ssh -p 2285 ucloud@ssh.cloud.sdu.dk "cd /work/Datadonationer && tmux has-session -t browser_pipeline 2>/dev/null && echo '✅ Running' || echo '❌ Stopped'"

echo ""
echo "📘 Facebook Pipeline:"
ssh -p 2285 ucloud@ssh.cloud.sdu.dk "cd /work/Datadonationer && tmux has-session -t facebook_pipeline 2>/dev/null && echo '✅ Running' || echo '❌ Stopped'"

echo ""
echo "📊 All Active Sessions:"
ssh -p 2285 ucloud@ssh.cloud.sdu.dk "tmux list-sessions"

echo ""
echo "💡 To monitor individual pipelines:"
echo "  Browser: ssh -p 2285 ucloud@ssh.cloud.sdu.dk 'cd /work/Datadonationer && ./monitor_pipeline.sh status'"
echo "  Facebook: ssh -p 2285 ucloud@ssh.cloud.sdu.dk 'cd /work/Datadonationer && ./monitor_facebook_pipeline.sh status'" 