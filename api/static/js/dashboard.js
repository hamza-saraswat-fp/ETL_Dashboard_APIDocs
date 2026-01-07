/**
 * Dashboard JavaScript
 * Utilities for the ETL Admin Panel
 */

// JSON syntax highlighting
function highlightJson(json) {
    if (typeof json !== 'string') {
        json = JSON.stringify(json, null, 2);
    }
    return json.replace(
        /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,
        function (match) {
            let cls = 'json-number';
            if (/^"/.test(match)) {
                if (/:$/.test(match)) {
                    cls = 'json-key';
                } else {
                    cls = 'json-string';
                }
            } else if (/true|false/.test(match)) {
                cls = 'json-boolean';
            } else if (/null/.test(match)) {
                cls = 'json-null';
            }
            return '<span class="' + cls + '">' + match + '</span>';
        }
    );
}

// Format relative time
function formatRelativeTime(dateString) {
    if (!dateString) return 'N/A';

    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now - date;
    const diffSec = Math.floor(diffMs / 1000);
    const diffMin = Math.floor(diffSec / 60);
    const diffHour = Math.floor(diffMin / 60);
    const diffDay = Math.floor(diffHour / 24);

    if (diffSec < 60) return 'just now';
    if (diffMin < 60) return `${diffMin}m ago`;
    if (diffHour < 24) return `${diffHour}h ago`;
    if (diffDay < 7) return `${diffDay}d ago`;

    return date.toLocaleDateString();
}

// Format duration
function formatDuration(ms) {
    if (!ms) return '--';

    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);

    if (hours > 0) {
        return `${hours}h ${minutes % 60}m`;
    }
    if (minutes > 0) {
        return `${minutes}m ${seconds % 60}s`;
    }
    return `${seconds}s`;
}

// Format number with commas
function formatNumber(num) {
    if (num === null || num === undefined) return '--';
    return num.toLocaleString();
}

// Apply JSON highlighting to all .json elements
function applyJsonHighlighting() {
    document.querySelectorAll('pre.json').forEach(function (block) {
        if (!block.dataset.highlighted) {
            block.innerHTML = highlightJson(block.textContent);
            block.dataset.highlighted = 'true';
        }
    });
}

// Update all relative timestamps
function updateRelativeTimes() {
    document.querySelectorAll('[data-timestamp]').forEach(function (el) {
        el.textContent = formatRelativeTime(el.dataset.timestamp);
    });
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function () {
    applyJsonHighlighting();
    updateRelativeTimes();

    // Update timestamps every minute
    setInterval(updateRelativeTimes, 60000);
});

// Re-apply after HTMX swaps
document.body.addEventListener('htmx:afterSwap', function (event) {
    applyJsonHighlighting();
    updateRelativeTimes();
});

// Handle HTMX request indicators
document.body.addEventListener('htmx:beforeRequest', function (event) {
    const target = event.target;
    const indicator = target.querySelector('.htmx-indicator');
    if (indicator) {
        indicator.classList.add('opacity-100');
    }
});

document.body.addEventListener('htmx:afterRequest', function (event) {
    const target = event.target;
    const indicator = target.querySelector('.htmx-indicator');
    if (indicator) {
        indicator.classList.remove('opacity-100');
    }
});

// Copy to clipboard utility
function copyToClipboard(text, button) {
    navigator.clipboard.writeText(text).then(function () {
        const originalText = button.textContent;
        button.textContent = 'Copied!';
        setTimeout(function () {
            button.textContent = originalText;
        }, 2000);
    });
}

// Toggle JSON collapse
function toggleJsonCollapse(element) {
    const content = element.nextElementSibling;
    if (content) {
        content.classList.toggle('hidden');
        element.querySelector('.collapse-icon')?.classList.toggle('rotate-90');
    }
}

// Diff viewer - compute simple diff for display
function computeSimpleDiff(json1, json2) {
    const str1 = JSON.stringify(json1, null, 2);
    const str2 = JSON.stringify(json2, null, 2);

    if (str1 === str2) {
        return { identical: true };
    }

    return {
        identical: false,
        size1: str1.length,
        size2: str2.length,
    };
}

// Export functions for global use
window.Dashboard = {
    highlightJson,
    formatRelativeTime,
    formatDuration,
    formatNumber,
    copyToClipboard,
    toggleJsonCollapse,
    computeSimpleDiff,
};
