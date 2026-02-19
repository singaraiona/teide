import type { DashboardWidget } from '../../store/useStore';
import TableWidget from './TableWidget';
import BarChartWidget from './BarChartWidget';
import LineChartWidget from './LineChartWidget';
import PieChartWidget from './PieChartWidget';
import StatCardWidget from './StatCardWidget';

interface Props {
  widget: DashboardWidget;
  data?: { rows: any[]; columns: string[] };
  selected: boolean;
  onRemove: () => void;
}

const widgetComponents: Record<string, React.FC<{ data?: { rows: any[]; columns: string[] }; config: Record<string, any> }>> = {
  table: TableWidget,
  bar_chart: BarChartWidget,
  line_chart: LineChartWidget,
  pie_chart: PieChartWidget,
  stat_card: StatCardWidget,
};

export default function WidgetCard({ widget, data, selected, onRemove }: Props) {
  const Component = widgetComponents[widget.type];

  return (
    <div className={`widget-card${selected ? ' selected' : ''}`}>
      <div className="widget-card-header">
        <span>{widget.title}</span>
        <button onClick={(e) => { e.stopPropagation(); onRemove(); }}>&times;</button>
      </div>
      <div className="widget-card-body">
        {Component ? (
          <Component data={data} config={widget.config} />
        ) : (
          <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>Unknown widget type</div>
        )}
      </div>
    </div>
  );
}
