import { useLocation, useNavigate } from 'react-router-dom';
import {
  SideNav,
  SideNavItems,
  SideNavLink,
} from '@carbon/react';
import {
  Dashboard,
  Table,
  DataBase,
  TaskTools,
  Search,
  Debug,
  Settings,
  Notification,
} from '@carbon/icons-react';
import './Navigation.scss';

export function Navigation() {
  const location = useLocation();
  const navigate = useNavigate();

  const navItems = [
    { path: '/', label: 'Overview', icon: Dashboard },
    { path: '/pipeline', label: 'Global Pipeline', icon: DataBase },
    { path: '/jobs', label: 'Job Queue', icon: TaskTools },
    { path: '/search', label: 'Semantic Search', icon: Search },
    { path: '/debug', label: 'Debug Panel', icon: Debug },
    { path: '/config', label: 'Configuration', icon: Settings },
    { path: '/alerts', label: 'Alerts', icon: Notification },
  ];

  const isActive = (path: string) => {
    if (path === '/') {
      return location.pathname === '/';
    }
    return location.pathname.startsWith(path);
  };

  return (
    <SideNav
      aria-label="Side navigation"
      expanded
      isFixedNav
      className="dashboard-sidenav"
    >
      <SideNavItems>
        {navItems.map(item => (
          <SideNavLink
            key={item.path}
            renderIcon={item.icon}
            onClick={() => navigate(item.path)}
            isActive={isActive(item.path)}
          >
            {item.label}
          </SideNavLink>
        ))}
      </SideNavItems>
    </SideNav>
  );
}
