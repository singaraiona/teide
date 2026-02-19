/** Production-grade icons from Font Awesome 6 + Tabler */
import {
  FaFileCsv,
  FaDatabase,
  FaFilter,
  FaLayerGroup,
  FaCodeMerge,
  FaArrowDownWideShort,
  FaTableCells,
  FaChartColumn,
  FaFileExport,
  FaFilePdf,
  FaWandMagicSparkles,
  FaPython,
  FaShuffle,
  FaEnvelope,
  FaGoogleDrive,
  FaRobot,
} from 'react-icons/fa6';
import { TbMathFunction } from 'react-icons/tb';

export function IconTeide() {
  return (
    <svg width="24" height="16" viewBox="0 0 48 32" fill="none">
      <path d="M0 31 L19.2 4.2 L22.3 7.7 L32.6 0 L37.4 6.4 L40 4.6 L47.7 17.6 L39.3 7.5 L37.1 9 L32.5 2.9 L30.7 15.7 L28.3 12.4 L23 22.3 L21.1 16.7 L13.8 24 L12.6 20.6 Z" fill="currentColor"/>
    </svg>
  );
}

/** Map node type IDs → icon components */
const iconMap: Record<string, React.ComponentType<{ size?: number }>> = {
  csv_source: FaFileCsv,
  query: FaDatabase,
  formula: TbMathFunction,
  conditional: FaShuffle,
  script: FaPython,
  dict_transform: FaWandMagicSparkles,
  grid: FaTableCells,
  chart: FaChartColumn,
  export: FaFileExport,
  pdf_render: FaFilePdf,
  filter: FaFilter,
  group_by: FaLayerGroup,
  join: FaCodeMerge,
  sort: FaArrowDownWideShort,
  ai: FaRobot,
  gmail: FaEnvelope,
  google_drive: FaGoogleDrive,
};

function IconFallback({ size = 16 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
      <circle cx="12" cy="12" r="9"/>
    </svg>
  );
}

export function NodeIcon({ nodeType, size = 16 }: { nodeType: string; size?: number }) {
  const Icon = iconMap[nodeType] ?? IconFallback;
  return <Icon size={size} />;
}
