import { useState } from 'react';
import {
  DataTable,
  Table,
  TableHead,
  TableRow,
  TableHeader,
  TableBody,
  TableCell,
  TableContainer,
  Tag,
  OverflowMenu,
  OverflowMenuItem,
  Button,
} from '@carbon/react';
import { Add } from '@carbon/icons-react';
import type { TableConfig } from '../types';
import './VectorizedTables.scss';

interface VectorizedTablesProps {
  tables: TableConfig[];
  loading: boolean;
  onSync: (tableId: string) => void;
  onDelete: (tableId: string) => void;
  onToggle: (tableId: string) => void;
}

export const VectorizedTables = ({
  tables,
  loading,
  onSync,
  onDelete,
  onToggle,
}: VectorizedTablesProps) => {
  const [syncing, setSyncing] = useState<Set<string>>(new Set());

  const handleSync = async (tableId: string) => {
    setSyncing(prev => new Set(prev).add(tableId));
    try {
      await onSync(tableId);
    } finally {
      setSyncing(prev => {
        const next = new Set(prev);
        next.delete(tableId);
        return next;
      });
    }
  };

  const headers = [
    { key: 'tableName', header: 'Table Name' },
    { key: 'catalog', header: 'Catalog' },
    { key: 'lastSync', header: 'Last Sync' },
    { key: 'model', header: 'Embedding Model' },
    { key: 'status', header: 'Status' },
    { key: 'actions', header: 'Actions' },
  ];

  const rows = tables.map(table => ({
    id: table.tableId,
    tableName: table.tableName,
    catalog: table.catalog,
    lastSync: table.createdAt
      ? new Date(table.createdAt).toLocaleString()
      : 'Never',
    model: table.modelName,
    status: table.enabled ? 'enabled' : 'disabled',
    isSyncing: syncing.has(table.tableId),
  }));

  if (loading) {
    return (
      <div className="vectorized-tables">
        <h3>Vectorized Tables</h3>
        <div className="loading">Loading tables...</div>
      </div>
    );
  }

  return (
    <div className="vectorized-tables">
      <div className="table-header">
        <h3>Vectorized Tables</h3>
        <Button
          size="sm"
          kind="primary"
          className="register-table-btn"
          renderIcon={Add}
        >
          Register New Table
        </Button>
      </div>

      <DataTable rows={rows} headers={headers}>
        {({ rows, headers, getTableProps, getHeaderProps, getRowProps }) => (
          <TableContainer>
            <Table {...getTableProps()} size="lg" useZebraStyles>
              <TableHead>
                <TableRow>
                  {headers.map(header => (
                    <TableHeader {...getHeaderProps({ header })} key={header.key}>
                      {header.header}
                    </TableHeader>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {rows.map(row => {
                  const tableData = tables.find(t => t.tableId === row.id);
                  const isSyncing = syncing.has(row.id);

                  return (
                    <TableRow {...getRowProps({ row })} key={row.id}>
                      {row.cells.map(cell => {
                        if (cell.info.header === 'status') {
                          return (
                            <TableCell key={cell.id}>
                              {isSyncing ? (
                                <Tag type="blue" size="sm">
                                  Syncing...
                                </Tag>
                              ) : tableData?.enabled ? (
                                <Tag type="green" size="sm">
                                  Enabled
                                </Tag>
                              ) : (
                                <Tag type="gray" size="sm">
                                  Disabled
                                </Tag>
                              )}
                            </TableCell>
                          );
                        }

                        if (cell.info.header === 'actions') {
                          return (
                            <TableCell key={cell.id}>
                              <OverflowMenu size="sm" flipped className="row-actions-menu">
                                <OverflowMenuItem
                                  itemText="Sync Now"
                                  onClick={() => handleSync(row.id)}
                                  disabled={isSyncing}
                                />
                                <OverflowMenuItem
                                  itemText={tableData?.enabled ? 'Disable' : 'Enable'}
                                  onClick={() => onToggle(row.id)}
                                />
                                <OverflowMenuItem
                                  itemText="Delete"
                                  onClick={() => onDelete(row.id)}
                                  isDelete
                                />
                              </OverflowMenu>
                            </TableCell>
                          );
                        }

                        return <TableCell key={cell.id}>{cell.value}</TableCell>;
                      })}
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </DataTable>

      {tables.length === 0 && (
        <div className="empty-state">
          <p>No tables registered yet.</p>
          <Button size="sm" kind="tertiary">
            Register Your First Table
          </Button>
        </div>
      )}
    </div>
  );
};
