/**
 * 美容室顧客データリピート分析システム - メインJavaScript
 */

// グローバル変数
let chartInstances = {};

// DOM読み込み完了後の初期化
document.addEventListener('DOMContentLoaded', function() {
    initializeApp();
});

/**
 * アプリケーション初期化
 */
function initializeApp() {
    // Bootstrap tooltips初期化
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
    
    // スムーススクロール
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({ behavior: 'smooth' });
            }
        });
    });
}

/**
 * チャート作成ヘルパー関数
 */
function createChart(canvasId, chartConfig) {
    console.log("Attempting to create chart:", canvasId, JSON.parse(JSON.stringify(chartConfig)));
    const ctx = document.getElementById(canvasId);
    if (!ctx) {
        console.error(`Canvas element with id '${canvasId}' not found`);
        return null;
    }
    
    // 既存のチャートがあれば破棄
    if (chartInstances[canvasId]) {
        chartInstances[canvasId].destroy();
    }
    
    // 新しいチャートを作成
    chartInstances[canvasId] = new Chart(ctx, chartConfig);
    return chartInstances[canvasId];
}

/**
 * 数値フォーマット関数
 */
function formatNumber(num, decimals = 0) {
    return new Intl.NumberFormat('ja-JP', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    }).format(num);
}

/**
 * パーセント表示関数
 */
function formatPercent(num, decimals = 1) {
    return formatNumber(num, decimals) + '%';
}

/**
 * APIエラーハンドリング
 */
function handleApiError(error) {
    console.error('API Error:', error);
    showAlert('エラーが発生しました: ' + error.message, 'danger');
}

/**
 * アラート表示
 */
function showAlert(message, type = 'info') {
    const alertContainer = document.getElementById('alert-container') || document.body;
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type} alert-dismissible fade show`;
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    alertContainer.appendChild(alertDiv);
    
    // 5秒後に自動削除
    setTimeout(() => {
        if (alertDiv.parentNode) {
            alertDiv.parentNode.removeChild(alertDiv);
        }
    }, 5000);
}

/**
 * ローディング表示/非表示
 */
function showLoading(show = true) {
    let loader = document.getElementById('loading-overlay');
    
    if (show) {
        if (!loader) {
            loader = document.createElement('div');
            loader.id = 'loading-overlay';
            loader.className = 'loading-overlay';
            loader.innerHTML = `
                <div class="loading-spinner">
                    <div class="spinner-border text-primary" role="status">
                        <span class="visually-hidden">読み込み中...</span>
                    </div>
                    <p class="mt-2">処理中...</p>
                </div>
            `;
            document.body.appendChild(loader);
        }
        loader.style.display = 'flex';
    } else {
        if (loader) {
            loader.style.display = 'none';
        }
    }
}

/**
 * CSVダウンロード
 */
function downloadCSV(data, filename = 'data.csv') {
    const blob = new Blob([data], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    
    if (link.download !== undefined) {
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', filename);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }
}

/**
 * 印刷機能
 */
function printPage() {
    window.print();
}

/**
 * ダッシュボード用のユーティリティ関数
 */
const Dashboard = {
    /**
     * サマリーカードを更新
     */
    updateSummaryCard: function(cardId, value, unit = '') {
        const card = document.getElementById(cardId);
        if (card) {
            const valueElement = card.querySelector('.card-value');
            if (valueElement) {
                valueElement.textContent = formatNumber(value);
            }
        }
    },
    
    /**
     * チャートデータを取得してレンダリング
     */
    loadChart: function(chartType, canvasId) {
        fetch(`/api/chart/${chartType}`)
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    console.error('Chart data error:', data.error);
                    return;
                }
                createChart(canvasId, data);
            })
            .catch(error => {
                console.error('Failed to load chart:', error);
            });
    },
    
    /**
     * すべてのチャートを再読み込み
     */
    refreshAllCharts: function() {
        Object.keys(chartInstances).forEach(canvasId => {
            if (chartInstances[canvasId]) {
                chartInstances[canvasId].update();
            }
        });
    }
};

/**
 * ファイルアップロード用のユーティリティ
 */
const FileUpload = {
    /**
     * ファイルサイズ検証
     */
    validateFileSize: function(file, maxSizeMB = 50) {
        const maxSize = maxSizeMB * 1024 * 1024;
        return file.size <= maxSize;
    },
    
    /**
     * ファイル拡張子検証
     */
    validateFileExtension: function(file, allowedExtensions = ['.csv']) {
        const fileName = file.name.toLowerCase();
        return allowedExtensions.some(ext => fileName.endsWith(ext));
    },
    
    /**
     * 複数ファイルの一括検証
     */
    validateFiles: function(files) {
        const errors = [];
        
        Array.from(files).forEach((file, index) => {
            if (!this.validateFileExtension(file)) {
                errors.push(`ファイル ${index + 1}: CSVファイルを選択してください`);
            }
            if (!this.validateFileSize(file)) {
                errors.push(`ファイル ${index + 1}: ファイルサイズが大きすぎます（最大50MB）`);
            }
        });
        
        return errors;
    }
};

/**
 * エクスポート機能
 */
const Export = {
    /**
     * レポートダウンロード
     */
    downloadReport: function() {
        showLoading(true);
        
        fetch('/report')
            .then(response => {
                if (response.ok) {
                    return response.blob();
                }
                throw new Error('レポート生成に失敗しました');
            })
            .then(blob => {
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = 'リピート分析レポート.txt';
                document.body.appendChild(a);
                a.click();
                window.URL.revokeObjectURL(url);
                document.body.removeChild(a);
                showAlert('レポートをダウンロードしました', 'success');
            })
            .catch(error => {
                handleApiError(error);
            })
            .finally(() => {
                showLoading(false);
            });
    }
};

// グローバルに公開
window.Dashboard = Dashboard;
window.FileUpload = FileUpload;
window.Export = Export;
window.showAlert = showAlert;
window.showLoading = showLoading;
window.createChart = createChart; 