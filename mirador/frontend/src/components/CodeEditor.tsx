import { useMemo, useEffect, useRef } from 'react';
import CodeMirror, { type ReactCodeMirrorRef } from '@uiw/react-codemirror';
import { python } from '@codemirror/lang-python';
import { javascript } from '@codemirror/lang-javascript';
import { sql } from '@codemirror/lang-sql';

const langFactories: Record<string, () => ReturnType<typeof python>> = {
  python: () => python(),
  javascript: () => javascript(),
  sql: () => sql(),
};

interface Props {
  value: string;
  onChange: (value: string) => void;
  language?: string;
  placeholder?: string;
  minHeight?: string;
  insertRef?: React.MutableRefObject<((text: string) => void) | null>;
}

export default function CodeEditor({
  value,
  onChange,
  language = 'python',
  placeholder,
  minHeight = '120px',
  insertRef,
}: Props) {
  const cmRef = useRef<ReactCodeMirrorRef>(null);

  const extensions = useMemo(() => {
    const factory = langFactories[language];
    return factory ? [factory()] : [];
  }, [language]);

  // Expose imperative insertAtCursor via ref
  useEffect(() => {
    if (!insertRef) return;
    insertRef.current = (text: string) => {
      const view = cmRef.current?.view;
      if (!view) return;
      const { from, to } = view.state.selection.main;
      view.dispatch({
        changes: { from, to, insert: text },
        selection: { anchor: from + text.length },
      });
      view.focus();
    };
  }, [insertRef]);

  return (
    <div className="cm-wrapper">
      <CodeMirror
        ref={cmRef}
        value={value}
        onChange={onChange}
        extensions={extensions}
        placeholder={placeholder}
        theme="light"
        minHeight={minHeight}
        basicSetup={{
          lineNumbers: true,
          foldGutter: false,
          highlightActiveLine: true,
          autocompletion: true,
          bracketMatching: true,
          closeBrackets: true,
        }}
      />
    </div>
  );
}
