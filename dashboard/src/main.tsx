import { render } from 'preact'
import { useEffect, useRef, useState } from 'preact/hooks'
import { createChart, ColorType, IChartApi, CandlestickData, HistogramData } from 'lightweight-charts'
import './style.css'

type AnyRecord = Record<string, any>

const api = async (path: string, options?: RequestInit) => {
  const response = await fetch(path, options)
  const data = await response.json()
  if (!response.ok) throw new Error(data.error || `请求失败（${response.status}）`)
  return data
}

function formatNumber(value: any) {
  if (value === null || value === undefined || value === '') return '--'
  if (typeof value === 'number') return value.toLocaleString('zh-CN', { maximumFractionDigits: 2 })
  return String(value)
}

function formatDate(value: any) {
  return value ? String(value).replace('T', ' ').slice(0, 19) : '--'
}

function Metric({ label, value, hint }: { label: string; value: any; hint?: string }) {
  return <div className="metric"><span>{label}</span><strong>{formatNumber(value)}</strong>{hint && <small>{hint}</small>}</div>
}

function App() {
  const [status, setStatus] = useState<AnyRecord | null>(null)
  const [coverage, setCoverage] = useState<AnyRecord | null>(null)
  const [errors, setErrors] = useState<AnyRecord[]>([])
  const [query, setQuery] = useState('')
  const [stocks, setStocks] = useState<AnyRecord[]>([])
  const [selected, setSelected] = useState<AnyRecord | null>(null)
  const [daily, setDaily] = useState<AnyRecord | null>(null)
  const [financials, setFinancials] = useState<AnyRecord | null>(null)
  const [dataset, setDataset] = useState('profit')
  const [loading, setLoading] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [message, setMessage] = useState('')
  const chartRef = useRef<HTMLDivElement>(null)
  const chartApiRef = useRef<IChartApi | null>(null)

  const loadStatus = async () => {
    try {
      const [nextStatus, nextCoverage, nextErrors] = await Promise.all([
        api('/api/dashboard/status'), api('/api/dashboard/coverage'), api('/api/dashboard/errors'),
      ])
      setStatus(nextStatus); setCoverage(nextCoverage); setErrors(nextErrors.errors || [])
    } catch (error) { setMessage((error as Error).message) }
  }

  useEffect(() => { loadStatus(); const timer = window.setInterval(loadStatus, 5000); return () => window.clearInterval(timer) }, [])

  useEffect(() => {
    const timer = window.setTimeout(async () => {
      if (!query.trim()) { setStocks([]); return }
      try { setStocks((await api(`/api/stocks?q=${encodeURIComponent(query.trim())}`)).data || []) }
      catch (error) { setMessage((error as Error).message) }
    }, 250)
    return () => window.clearTimeout(timer)
  }, [query])

  const loadStock = async (stock: AnyRecord) => {
    setSelected(stock); setLoading(true); setMessage('')
    try {
      const [nextDaily, nextFinancials] = await Promise.all([
        api(`/api/stocks/${stock.code}/daily?adjustflag=3`),
        api(`/api/stocks/${stock.code}/financials?dataset=${dataset}`),
      ])
      setDaily(nextDaily); setFinancials(nextFinancials)
    } catch (error) { setMessage((error as Error).message) }
    finally { setLoading(false) }
  }

  useEffect(() => {
    if (!selected) return
    api(`/api/stocks/${selected.code}/financials?dataset=${dataset}`)
      .then(setFinancials).catch((error) => setMessage(error.message))
  }, [dataset])

  const refresh = async () => {
    if (!selected) return
    setRefreshing(true); setMessage('正在通过优先级队列刷新远端数据…')
    try {
      const result = await api(`/api/stocks/${selected.code}/daily/refresh?adjustflag=3`, { method: 'POST' })
      setDaily({ ...daily, rows: result.rows, coverage: result.coverage })
      setMessage(result.remote_requested ? '远端数据刷新完成' : '本地数据已经覆盖请求范围，无需访问远端')
      await loadStatus()
    } catch (error) { setMessage((error as Error).message) }
    finally { setRefreshing(false) }
  }

  useEffect(() => {
    if (!chartRef.current) return
    chartApiRef.current?.remove()
    const chart = createChart(chartRef.current, {
      layout: { background: { type: ColorType.Solid, color: '#111a2b' }, textColor: '#9eacc3' },
      grid: { vertLines: { color: '#1e2a40' }, horzLines: { color: '#1e2a40' } },
      width: chartRef.current.clientWidth, height: 360,
      rightPriceScale: { borderColor: '#33415c' }, timeScale: { borderColor: '#33415c' },
    })
    chartApiRef.current = chart
    if (daily?.rows?.length) {
      const candles: CandlestickData[] = daily.rows.filter((row: AnyRecord) => row.date && row.open != null).map((row: AnyRecord) => ({
        time: row.date, open: Number(row.open), high: Number(row.high), low: Number(row.low), close: Number(row.close),
      }))
      const volumes: HistogramData[] = daily.rows.filter((row: AnyRecord) => row.date && row.volume != null).map((row: AnyRecord) => ({ time: row.date, value: Number(row.volume), color: Number(row.close) >= Number(row.open) ? '#ef6a75' : '#43c59e' }))
      chart.addCandlestickSeries({ upColor: '#ef6a75', downColor: '#43c59e', borderVisible: false, wickUpColor: '#ef6a75', wickDownColor: '#43c59e' }).setData(candles)
      chart.addHistogramSeries({ priceFormat: { type: 'volume' }, priceScaleId: '' }).setData(volumes)
      chart.timeScale().fitContent()
    }
    const resize = () => chart.applyOptions({ width: chartRef.current?.clientWidth || 700 })
    window.addEventListener('resize', resize)
    return () => { window.removeEventListener('resize', resize); chart.remove(); chartApiRef.current = null }
  }, [daily])

  const fetcher = status?.fetcher || {}
  const budget = status?.budget || {}
  const jobs = status?.jobs || {}
  const financialRows = financials?.rows || []
  const financialColumns = Array.from(new Set(financialRows.flatMap((row: AnyRecord) => Object.keys(row.data || {})))).slice(0, 12)

  return <main className="shell">
    <header className="topbar"><div><p className="eyebrow">BAOSTOCK RUNNER</p><h1>数据服务控制台</h1></div><div className="health"><i className={status ? 'ok' : 'bad'} />{status ? '服务在线' : '连接中'}<small>{status ? formatDate(status.server_time) : ''}</small></div></header>

    <section className="metrics">
      <Metric label="Fetcher 状态" value={fetcher.current || '未启动'} hint={fetcher.dataset || '等待任务'} />
      <Metric label="今日后台请求" value={budget.fetcher_used_today} hint={`剩余 ${formatNumber(budget.fetcher_left)}`} />
      <Metric label="队列长度" value={status?.gateway?.queue_length ?? '--'} hint={status?.gateway?.circuit_breaker_open ? '熔断已打开' : '正常'} />
      <Metric label="股票数量" value={coverage?.securities} hint={`日线 ${formatNumber(coverage?.daily_bars?.rows)} 条`} />
    </section>

    <section className="grid two">
      <article className="panel"><div className="panel-title"><h2>Fetcher 进度</h2><span className={fetcher.running ? 'badge active' : 'badge'}>{fetcher.running ? '运行中' : '空闲/暂停'}</span></div><div className="progress"><span style={{ width: `${fetcher.total ? Math.min(100, fetcher.done / fetcher.total * 100) : 0}%` }} /></div><div className="split"><span>{formatNumber(fetcher.done)} / {formatNumber(fetcher.total)}</span><span>{fetcher.updated_at ? formatDate(fetcher.updated_at) : '--'}</span></div><p className="muted">{fetcher.last_error || fetcher.current || '暂无状态'}</p><div className="job-list">{Object.entries(jobs).map(([name, value]: [string, any]) => <div className="job" key={name}><span>{name}</span><b>{value.done || 0}</b><small>/ {value.total || 0} 完成</small></div>)}</div></article>
      <article className="panel"><div className="panel-title"><h2>数据覆盖率</h2><span className="muted">本地 SQLite</span></div><div className="coverage"><div><b>{formatNumber(coverage?.daily_bars?.codes)}</b><span>日线覆盖股票</span></div><div><b>{formatNumber(coverage?.financials?.codes)}</b><span>财务覆盖股票</span></div><div><b>{formatNumber(coverage?.financials?.rows)}</b><span>财务记录</span></div></div><dl><dt>日线范围</dt><dd>{coverage?.daily_bars?.start_date || '--'} → {coverage?.daily_bars?.end_date || '--'}</dd><dt>财务年份</dt><dd>{coverage?.financials?.start_year || '--'} → {coverage?.financials?.end_year || '--'}</dd><dt>最后更新</dt><dd>{formatDate(coverage?.financials?.updated_at)}</dd></dl></article>
    </section>

    <section className="panel stock-panel"><div className="panel-title"><h2>个股查询</h2><span className="muted">默认只读本地数据</span></div><div className="search-row"><input value={query} onInput={(event) => setQuery((event.target as HTMLInputElement).value)} placeholder="输入股票代码或名称，例如 600000 / 浦发" /><span>{loading ? '加载中…' : selected ? `${selected.code} ${selected.name || ''}` : '请选择股票'}</span></div>{stocks.length > 0 && <div className="results">{stocks.map((stock) => <button key={stock.code} onClick={() => { setQuery(''); loadStock(stock) }}><b>{stock.code}</b><span>{stock.name || '未命名'}</span></button>)}</div>}{message && <div className="notice">{message}</div>}{selected && <><div className="stock-toolbar"><h3>{selected.name || selected.code} <small>{selected.code}</small></h3><button className="primary" onClick={refresh} disabled={refreshing}>{refreshing ? '刷新中…' : '刷新远端数据'}</button></div><div ref={chartRef} className="chart" /><div className="table-title"><h3>财务数据</h3><select value={dataset} onChange={(event) => setDataset((event.target as HTMLSelectElement).value)}><option value="profit">利润表</option><option value="growth">成长能力</option><option value="balance">资产负债表</option><option value="cash_flow">现金流量表</option></select></div><div className="table-wrap"><table><thead><tr><th>报告期</th>{financialColumns.map((column) => <th key={column}>{column}</th>)}</tr></thead><tbody>{financialRows.map((row: AnyRecord) => <tr key={`${row.year}-${row.quarter}`}><td>{row.year} Q{row.quarter}</td>{financialColumns.map((column) => <td key={column}>{formatNumber(row.data?.[column])}</td>)}</tr>)}</tbody></table>{!financialRows.length && <p className="muted empty">暂无本地财务数据</p>}</div></>}
    </section>
    {errors.length > 0 && <section className="panel errors"><div className="panel-title"><h2>最近错误</h2><span className="badge bad-text">{errors.length}</span></div>{errors.slice(0, 5).map((error) => <div className="error-row" key={`${error.dataset}-${error.batch_id}-${error.updated_at}`}><b>{error.dataset}</b><span>{error.batch_id}</span><small>{error.error || '未知错误'} · {formatDate(error.updated_at)}</small></div>)}</section>}
  </main>
}

render(<App />, document.getElementById('app')!)
