import { useLocation, useNavigate } from 'react-router-dom';
import {
  SideNav,
  SideNavItems,
  SideNavLink,
} from '@carbon/react';
import {
  Dashboard,
  Settings,
} from '@carbon/icons-react';
import './Navigation.scss';

export function Navigation() {
  const location = useLocation();
  const navigate = useNavigate();

  // Only routes backed by real endpoints. Pages for jobs, alerts, workers and pipeline metrics
  // were removed along with the fabricated data that populated them.
  const navItems = [
    { path: '/', label: 'Overview', icon: Dashboard },
    { path: '/config', label: 'Configuration', icon: Settings },
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
