import { useState, useEffect, useRef } from "react";
import {
  Alert,
  Button,
  Card,
  Col,
  DatePicker,
  Descriptions,
  message,
  Row,
  Select,
  Space,
  Spin,
  Tag,
  Typography,
} from "antd";
import {
  ThunderboltOutlined,
  DownloadOutlined,
} from "@ant-design/icons";
import dayjs from "dayjs";
import type { Dayjs } from "dayjs";
import {
  listStrategies,
  listGroups,
  generateSignals,
  getTaskStatus,
  getSignalRun,
  exportSignals,
} from "../../api";
import type {
  Strategy,
  StockGroup,
  SignalRun,
} from "../../api";
import SignalTable from "./SignalTable";

const { Text } = Typography;

import type { SignalRestoreConfig } from "./SignalHistory";

interface SignalGeneratorPanelProps {
  onSignalComplete?: () => void;
  restoreConfig?: SignalRestoreConfig | null;
}

export default function SignalGeneratorPanel({ onSignalComplete, restoreConfig }: SignalGeneratorPanelProps) {
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [groups, setGroups] = useState<StockGroup[]>([]);

  const [selectedStrategy, setSelectedStrategy] = useState<string>("");
  const [selectedGroup, setSelectedGroup] = useState<string>("");
  const [targetDate, setTargetDate] = useState<Dayjs>(dayjs());

  const [running, setRunning] = useState(false);
  const [runError, setRunError] = useState<string | null>(null);
  const [result, setResult] = useState<SignalRun | null>(null);

  const [messageApi, contextHolder] = message.useMessage();
  const pollRef = useRef<ReturnType<typeof setInterval> | undefined>(undefined);

  useEffect(() => {
    listStrategies().then(setStrategies).catch(() => {});
    listGroups().then(setGroups).catch(() => {});
  }, []);

  // Restore config from signal history
  useEffect(() => {
    if (!restoreConfig) return;
    setSelectedStrategy(restoreConfig.strategyId);
    if (restoreConfig.groupId) setSelectedGroup(restoreConfig.groupId);
    if (restoreConfig.targetDate) setTargetDate(dayjs(restoreConfig.targetDate));
  }, [restoreConfig]);

  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  const handleGenerate = async () => {
    if (!selectedStrategy) {
      messageApi.warning("请选择策略");
      return;
    }
    if (!selectedGroup) {
      messageApi.warning("请选择股票分组");
      return;
    }

    setRunning(true);
    setRunError(null);
    setResult(null);

    try {
      const { task_id } = await generateSignals({
        strategy_id: selectedStrategy,
        target_date: targetDate.format("YYYY-MM-DD"),
        universe_group_id: selectedGroup,
      });

      // Poll task status
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = setInterval(async () => {
        try {
          const status = await getTaskStatus(task_id);
          if (status.status === "completed") {
            if (pollRef.current) clearInterval(pollRef.current);
            pollRef.current = undefined;

            const runId = (status.result as Record<string, string> | null)?.id;
            if (runId) {
              const detail = await getSignalRun(runId);
              setResult(detail);
            }
            setRunning(false);
            messageApi.success("信号生成完成");
            onSignalComplete?.();
          } else if (status.status === "failed") {
            if (pollRef.current) clearInterval(pollRef.current);
            pollRef.current = undefined;
            setRunError(status.error ?? "信号生成失败");
            setRunning(false);
          }
        } catch {
          // Keep polling on transient errors
        }
      }, 3000);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "信号生成失败";
      setRunError(msg);
      setRunning(false);
    }
  };

  const handleExport = async (format: "csv" | "json") => {
    if (!result) return;
    try {
      await exportSignals(result.id, format);
      messageApi.success(`导出${format.toUpperCase()}成功`);
    } catch {
      messageApi.error("导出失败");
    }
  };

  return (
    <>
      {contextHolder}
      <Space direction="vertical" style={{ width: "100%" }} size="middle">
        <Card title="信号配置" size="small">
          <Space direction="vertical" style={{ width: "100%" }} size="small">
            <Row gutter={12}>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: 12 }}>策略</Text>
                <Select
                  style={{ width: "100%" }}
                  placeholder="选择策略..."
                  value={selectedStrategy || undefined}
                  onChange={setSelectedStrategy}
                  options={strategies.map((s) => ({
                    value: s.id,
                    label: `${s.name} v${s.version} (${s.status})`,
                  }))}
                  showSearch
                  optionFilterProp="label"
                />
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: 12 }}>股票分组</Text>
                <Select
                  style={{ width: "100%" }}
                  placeholder="选择分组..."
                  value={selectedGroup || undefined}
                  onChange={setSelectedGroup}
                  options={groups.map((g) => ({
                    value: g.id,
                    label: `${g.name} (${g.member_count})`,
                  }))}
                  showSearch
                  optionFilterProp="label"
                />
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: 12 }}>目标日期</Text>
                <DatePicker
                  style={{ width: "100%" }}
                  value={targetDate}
                  onChange={(v) => {
                    if (v) setTargetDate(v);
                  }}
                />
              </Col>
            </Row>
          </Space>
        </Card>

        <Button
          type="primary"
          icon={<ThunderboltOutlined />}
          loading={running}
          onClick={handleGenerate}
          block
          style={{ background: "#1677ff", borderColor: "#1677ff" }}
          size="large"
        >
          生成信号
        </Button>

        {running && (
          <Card size="small">
            <div style={{ textAlign: "center", padding: 24 }}>
              <Spin size="large" />
              <div style={{ marginTop: 12 }}>
                <Text type="secondary">正在生成信号...</Text>
              </div>
            </div>
          </Card>
        )}

        {runError && (
          <Alert
            type="error"
            showIcon
            message="信号生成错误"
            description={runError}
            closable
            onClose={() => setRunError(null)}
          />
        )}

        {result && (
          <Space direction="vertical" style={{ width: "100%" }} size="middle">
            {/* Result Level Badge */}
            <Card size="small">
              <Space size="middle">
                <Text strong>结果等级:</Text>
                <Tag color={result.result_level === "formal" ? "green" : "orange"}>
                  {result.result_level === "formal" ? "正式" : "探索性"}
                </Tag>
                <Text type="secondary">
                  共 {result.signal_count} 条信号
                </Text>
              </Space>
            </Card>

            {/* Warnings */}
            {result.warnings && result.warnings.length > 0 && (
              <Alert
                type="warning"
                showIcon
                message="警告信息"
                description={
                  <ul style={{ margin: 0, paddingLeft: 20 }}>
                    {result.warnings.map((w, i) => (
                      <li key={i}>{w}</li>
                    ))}
                  </ul>
                }
              />
            )}

            {/* Dependency Snapshot */}
            {result.dependency_snapshot && (
              <Card title="依赖快照" size="small">
                <Descriptions size="small" column={2} bordered>
                  <Descriptions.Item label="策略版本">
                    {(result.dependency_snapshot as Record<string, unknown>).strategy_version as string ?? result.strategy_version}
                  </Descriptions.Item>
                  <Descriptions.Item label="目标日期">
                    {result.target_date}
                  </Descriptions.Item>
                  {(result.dependency_snapshot as Record<string, unknown>).factors_used != null && (
                    <Descriptions.Item label="使用因子" span={2}>
                      {String((result.dependency_snapshot as Record<string, unknown>).factors_used)}
                    </Descriptions.Item>
                  )}
                  {(result.dependency_snapshot as Record<string, unknown>).models_used != null && (
                    <Descriptions.Item label="使用模型" span={2}>
                      {String((result.dependency_snapshot as Record<string, unknown>).models_used)}
                    </Descriptions.Item>
                  )}
                  {(result.dependency_snapshot as Record<string, unknown>).data_freshness != null && (
                    <Descriptions.Item label="数据新鲜度" span={2}>
                      {String((result.dependency_snapshot as Record<string, unknown>).data_freshness)}
                    </Descriptions.Item>
                  )}
                </Descriptions>
              </Card>
            )}

            {/* Signals Table */}
            <SignalTable signals={result.signals ?? []} />

            {/* Export Buttons */}
            <Space>
              <Button
                icon={<DownloadOutlined />}
                onClick={() => handleExport("csv")}
              >
                导出CSV
              </Button>
              <Button
                icon={<DownloadOutlined />}
                onClick={() => handleExport("json")}
              >
                导出JSON
              </Button>
            </Space>
          </Space>
        )}
      </Space>
    </>
  );
}
