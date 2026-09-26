import { render } from 'preact'
import { useEffect, useRef, useState } from 'preact/hooks'
import { createChart, ColorType, IChartApi, CandlestickData, HistogramData } from 'lightweight-charts'
import './style.css'

type AnyRecord = Record<string, any>

const api = async (path: string, options?: RequestInit) => {
  const token = sessionStorage.getItem('baostock_api_token') || ''
  const response = await fetch(path, { ...options, headers: { ...(options?.headers || {}), ...(token ? { Authorization: `Bearer ${token}` } : {}) } })
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
  const [token, setToken] = useState(sessionStorage.getItem('baostock_api_token') || '')
  const [researchDate, setResearchDate] = useState(new Date().toISOString().slice(0, 10))
  const [researchCodes, setResearchCodes] = useState('')
  const [researchMode, setResearchMode] = useState('revised_history')
  const [screen, setScreen] = useState<AnyRecord | null>(null)
  const [backtest, setBacktest] = useState<AnyRecord | null>(null)
  const [backtestEnd, setBacktestEnd] = useState(new Date().toISOString().slice(0, 10))
  const [boundariesText, setBoundariesText] = useState('{}')
  const stockRequest = useRef(0)
  const searchRequest = useRef(0)
  const researchDateInitialized = useRef(false)
  const chartRef = useRef<HTMLDivElement>(null)
  const chartApiRef = useRef<IChartApi | null>(null)

  const loadStatus = async () => {
    try {
      const [nextStatus, nextCoverage, nextErrors] = await Promise.all([
        api('/api/dashboard/status'), api('/api/dashboard/coverage'), api('/api/dashboard/errors'),
      ])
      setStatus(nextStatus); setCoverage(nextCoverage); setErrors(nextErrors.errors || [])
      if (!researchDateInitialized.current && nextStatus.latest_trade_day) { setResearchDate(nextStatus.latest_trade_day); researchDateInitialized.current = true }
    } catch (error) { setStatus(null); setCoverage(null); setErrors([]); setMessage((error as Error).message) }
  }

  useEffect(() => { loadStatus(); const timer = window.setInterval(loadStatus, 5000); return () => window.clearInterval(timer) }, [])

  useEffect(() => {
    const requestId = ++searchRequest.current
    const timer = window.setTimeout(async () => {
      if (!query.trim()) { setStocks([]); return }
      try { const next = (await api(`/api/stocks?q=${encodeURIComponent(query.trim())}`)).data || []; if (requestId === searchRequest.current) setStocks(next) }
      catch (error) { setMessage((error as Error).message) }
    }, 250)
    return () => window.clearTimeout(timer)
  }, [query])

  const loadStock = async (stock: AnyRecord) => {
    const requestId = ++stockRequest.current
    searchRequest.current++; setStocks([])
    setSelected(stock); setDaily(null); setFinancials(null); setLoading(true); setMessage('')
    try {
      const [nextDaily, nextFinancials] = await Promise.all([
        api(`/api/stocks/${stock.code}/daily?adjustflag=3`),
        api(`/api/stocks/${stock.code}/financials?dataset=${dataset}`),
      ])
      if (requestId === stockRequest.current) { setDaily(nextDaily); setFinancials(nextFinancials) }
    } catch (error) { setMessage((error as Error).message) }
    finally { if (requestId === stockRequest.current) setLoading(false) }
  }

  useEffect(() => {
    if (!selected) return
    let active = true
    api(`/api/stocks/${selected.code}/financials?dataset=${dataset}`)
      .then((value) => { if (active) setFinancials(value) }).catch((error) => { if (active) setMessage(error.message) })
    return () => { active = false }
  }, [dataset])

  const refresh = async () => {
    if (!selected) return
    setRefreshing(true); setMessage('刷新任务已提交…')
    try {
      const result = await api(`/api/stocks/${selected.code}/daily/refresh?adjustflag=3`, { method: 'POST' })
      for (let i = 0; i < 120; i++) {
        await new Promise((resolve) => setTimeout(resolve, 1000))
        const job = await api(`/api/jobs/${result.job_id}`)
        if (job.status === 'failed') throw new Error(job.error || '刷新失败')
        if (job.status === 'succeeded') break
      }
      setDaily(await api(`/api/stocks/${selected.code}/daily?adjustflag=3`))
      setMessage('远端数据刷新完成')
      await loadStatus()
    } catch (error) { setMessage((error as Error).message) }
    finally { setRefreshing(false) }
  }

  const runJob = async (path: string, body: AnyRecord) => {
    const queued = await api(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
    for (let i = 0; i < 120; i++) {
      await new Promise((resolve) => setTimeout(resolve, 500))
      const job = await api(`/api/jobs/${queued.job_id}`)
      if (job.status === 'failed') throw new Error(job.error || '任务失败')
      if (job.status === 'succeeded') return job.result
    }
    throw new Error('任务仍在运行，请稍后通过任务编号查看')
  }
  const runScreen = async () => {
    try {
      setMessage('筛选任务运行中…')
      const codes = researchCodes.split(/[\s,，]+/).filter(Boolean)
      const result = await runJob('/api/research/screens', { asof_date: researchDate, mode: researchMode, ...(codes.length ? { codes } : {}) })
      setScreen(result); setMessage(`筛选完成：${result.run_id}`)
    } catch (error) { setMessage((error as Error).message) }
  }
  const runBacktest = async () => {
    if (!screen) return
    try {
      setMessage('回测任务运行中…')
      const result = await runJob('/api/research/backtests', { screen_run_ids: [screen.run_id], end_date: backtestEnd, boundaries: JSON.parse(boundariesText) })
      setBacktest(result); setMessage(`回测完成：${result.run_id}`)
    } catch (error) { setMessage((error as Error).message) }
  }
  const download = async (kind: string, runId: string) => {
    try {
      const data = await api(`/api/research/export/${kind}/${runId}`)
      const url = URL.createObjectURL(new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' }))
      const link = document.createElement('a'); link.href = url; link.download = `${kind}-${runId}.json`; link.click()
      URL.revokeObjectURL(url)
    } catch (error) { setMessage((error as Error).message) }
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
  const financialColumns: string[] = Array.from(new Set<string>(financialRows.flatMap((row: AnyRecord) => Object.keys(row.data || {})))).slice(0, 12)

  return <main className="shell">
    <header className="topbar"><div><p className="eyebrow">BAOSTOCK RUNNER</p><h1>数据服务控制台</h1></div><div className="health"><i className={status ? 'ok' : 'bad'} />{status ? '服务在线' : '连接失败'}<small>{status ? formatDate(status.server_time) : ''}</small></div><input type="password" aria-label="API 令牌" placeholder="API 令牌" value={token} onInput={(e) => { const next = (e.target as HTMLInputElement).value; setToken(next); sessionStorage.setItem('baostock_api_token', next) }} /></header>

    <section className="metrics">
      <Metric label="Fetcher 状态" value={fetcher.current || '未启动'} hint={fetcher.dataset || '等待任务'} />
      <Metric label="今日后台请求" value={budget.fetcher_used_today} hint={`剩余 ${formatNumber(budget.fetcher_left)}`} />
      <Metric label="队列长度" value={status?.gateway?.queue_length ?? '--'} hint={status?.gateway?.circuit_breaker_open ? '熔断已打开' : '正常'} />
      <Metric label="股票数量" value={coverage?.securities} hint={`日线 ${formatNumber(coverage?.daily_bars?.rows)} 条`} />
    </section>
    <section className="panel"><div className="panel-title"><h2>研究筛选与回测</h2><span className="muted">本地数据 · 复权近似收益</span></div>
      <div className="search-row"><input type="date" value={researchDate} onInput={(e) => setResearchDate((e.target as HTMLInputElement).value)} /><input value={researchCodes} onInput={(e) => setResearchCodes((e.target as HTMLInputElement).value)} placeholder="固定股票池代码，逗号分隔；留空使用历史沪深300" /><select value={researchMode} onChange={(e) => setResearchMode((e.target as HTMLSelectElement).value)}><option value="revised_history">修订历史</option><option value="point_in_time">严格时点</option></select><button onClick={runScreen}>运行筛选</button></div>
      {screen && <><p>规则 {screen.rule_version} · 质量 {screen.quality} · 股票池 {screen.universe?.source} · {screen.results?.length} 只评估</p><input type="date" value={backtestEnd} onInput={(e) => setBacktestEnd((e.target as HTMLInputElement).value)} /><textarea aria-label="涨跌停边界 JSON" value={boundariesText} onInput={(e) => setBoundariesText((e.target as HTMLTextAreaElement).value)} placeholder='{"2026-09-28":{"sh.600000":{"up":12,"down":8}}}' /><button onClick={runBacktest}>运行回测</button><button onClick={() => download('screen', screen.run_id)}>导出筛选 JSON</button><div className="table-wrap"><table><thead><tr><th>代码</th><th>结果</th><th>动量</th><th>原因</th></tr></thead><tbody>{screen.results?.map((r: AnyRecord) => <tr key={r.code}><td>{r.code}</td><td>{r.status}</td><td>{formatNumber(r.factors?.momentum_20)}</td><td>{r.reasons?.join(', ') || '--'}</td></tr>)}</tbody></table></div></>}
      {backtest && <><p>质量 {backtest.quality} · 原因 {backtest.quality_reasons?.join(', ') || '--'} · 模型 {backtest.model} · 费用 {backtest.assumptions?.fee_bps}bp · 滑点 {backtest.assumptions?.slippage_bps}bp · 累计收益 {formatNumber(backtest.metrics?.cumulative_return)}</p><button onClick={() => download('backtest', backtest.run_id)}>导出回测 JSON</button><div className="table-wrap"><table><thead><tr><th>日期</th><th>净值</th><th>现金</th><th>原因</th></tr></thead><tbody>{backtest.daily?.map((r: AnyRecord) => <tr key={r.date}><td>{r.date}</td><td>{formatNumber(r.nav)}</td><td>{formatNumber(r.cash)}</td><td>{r.reasons?.map((x: AnyRecord) => `${x.code}: ${x.reason}`).join(', ') || '--'}</td></tr>)}</tbody></table></div></>}
    </section>

    <section className="grid two">
      <article className="panel"><div className="panel-title"><h2>Fetcher 进度</h2><span className={fetcher.running ? 'badge active' : 'badge'}>{fetcher.running ? '运行中' : '空闲/暂停'}</span></div><div className="progress"><span style={{ width: `${fetcher.total ? Math.min(100, fetcher.done / fetcher.total * 100) : 0}%` }} /></div><div className="split"><span>{formatNumber(fetcher.done)} / {formatNumber(fetcher.total)}</span><span>{fetcher.updated_at ? formatDate(fetcher.updated_at) : '--'}</span></div><p className="muted">{fetcher.last_error || fetcher.current || '暂无状态'}</p><div className="job-list">{Object.entries(jobs).map(([name, value]: [string, any]) => <div className="job" key={name}><span>{name}</span><b>{value.done || 0}</b><small>/ {value.total || 0} 完成</small></div>)}</div></article>
      <article className="panel"><div className="panel-title"><h2>数据覆盖率</h2><span className="muted">本地 SQLite</span></div><div className="coverage"><div><b>{formatNumber(coverage?.daily_bars?.codes)}</b><span>日线覆盖股票</span></div><div><b>{formatNumber(coverage?.financials?.codes)}</b><span>财务覆盖股票</span></div><div><b>{formatNumber(coverage?.financials?.rows)}</b><span>财务记录</span></div></div><dl><dt>日线范围</dt><dd>{coverage?.daily_bars?.start_date || '--'} → {coverage?.daily_bars?.end_date || '--'}</dd><dt>财务年份</dt><dd>{coverage?.financials?.start_year || '--'} → {coverage?.financials?.end_year || '--'}</dd><dt>最后更新</dt><dd>{formatDate(coverage?.financials?.updated_at)}</dd></dl></article>
    </section>

    <section className="panel stock-panel"><div className="panel-title"><h2>个股查询</h2><span className="muted">默认只读本地数据</span></div><div className="search-row"><input value={query} onInput={(event) => setQuery((event.target as HTMLInputElement).value)} placeholder="输入股票代码或名称，例如 600000 / 浦发" /><span>{loading ? '加载中…' : selected ? `${selected.code} ${selected.name || ''}` : '请选择股票'}</span></div>{stocks.length > 0 && <div className="results">{stocks.map((stock) => <button key={stock.code} onClick={() => { setQuery(''); loadStock(stock) }}><b>{stock.code}</b><span>{stock.name || '未命名'}</span></button>)}</div>}{message && <div className="notice">{message}</div>}{selected && <><div className="stock-toolbar"><h3>{selected.name || selected.code} <small>{selected.code}</small></h3><button className="primary" onClick={refresh} disabled={refreshing}>{refreshing ? '刷新中…' : '刷新远端数据'}</button></div><p className="muted">覆盖状态：{daily?.metadata?.coverage?.status || '--'} · 缺口 {formatNumber(daily?.metadata?.coverage?.unknown)} · 日线最后成功 {formatDate(status?.last_success?.daily_bars?.last_updated)}</p><div ref={chartRef} className="chart" /><div className="table-title"><h3>财务数据</h3><select value={dataset} onChange={(event) => setDataset((event.target as HTMLSelectElement).value)}><option value="profit">利润表</option><option value="growth">成长能力</option><option value="balance">资产负债表</option><option value="cash_flow">现金流量表</option></select></div><div className="table-wrap"><table><thead><tr><th>报告期</th>{financialColumns.map((column) => <th key={column}>{column}</th>)}</tr></thead><tbody>{financialRows.map((row: AnyRecord) => <tr key={`${row.year}-${row.quarter}`}><td>{row.year} Q{row.quarter}</td>{financialColumns.map((column) => <td key={column}>{formatNumber(row.data?.[column])}</td>)}</tr>)}</tbody></table>{!financialRows.length && <p className="muted empty">暂无本地财务数据</p>}</div></>}
    </section>
    {errors.length > 0 && <section className="panel errors"><div className="panel-title"><h2>最近错误</h2><span className="badge bad-text">{errors.length}</span></div>{errors.slice(0, 5).map((error) => <div className="error-row" key={`${error.dataset}-${error.batch_id}-${error.updated_at}`}><b>{error.dataset}</b><span>{error.batch_id}</span><small>{error.error || '未知错误'} · {formatDate(error.updated_at)} · 下次重试 {formatDate(error.next_retry_at)}</small></div>)}</section>}
  </main>
}

render(<App />, document.getElementById('app')!)
