import { render, screen, waitFor } from '@testing-library/react';

import { JobsWidget } from '@/components/JobsWidget';

jest.mock('@/lib/api', () => ({
  fetchActiveRuns: async () => [],
}));

describe('JobsWidget', () => {
  it('renders without crashing', async () => {
    render(<JobsWidget />);

    await waitFor(() => {
      expect(screen.getByText(/No active runs/i)).toBeInTheDocument();
    });
  });
});
