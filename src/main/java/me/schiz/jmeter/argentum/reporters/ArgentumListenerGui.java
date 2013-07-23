package me.schiz.jmeter.argentum.reporters;

import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.visualizers.gui.AbstractListenerGui;

import javax.swing.*;
import java.awt.*;

public class ArgentumListenerGui extends AbstractListenerGui {

    private JPanel jGeneralPanel;
    private JTextField jOutputFile;
    private JTextField jTimeout;
    private JTextField jPercentiles;
    private JTextField jTimePeriods;

    public ArgentumListenerGui() {
        super();
        init();
    }

    @Override
    public String getStaticLabel() {
        return "Argentum Listener";
    }

    @Override
    public String getLabelResource() {
        return getClass().getCanonicalName();
    }

    @Override
    public TestElement createTestElement() {
        TestElement te = new ArgentumListener();
        jTimeout.setText("10");
        jOutputFile.setText("/tmp/ag.txt");
        jPercentiles.setText("0.25 0.5 0.75 0.8 0.9 0.95 0.98 0.99 1.0");
        jTimePeriods.setText("1 2 3 4 5 6 7 8 9 10 20 30 40 50 60 70 80 90 100 150 200 250 300 350 400 450 500 600 650 700 750 800 850 900 950 1000 1500 2000 2500 3000 3500 4000 4500 5000 5500 6000 6500 7000 7500 8000 8500 9000 9500 10000 11000");

        modifyTestElement(te);
        return te;
    }
    public void configure(TestElement te) {
        super.configure(te);
        if(te instanceof ArgentumListener) {
            ArgentumListener listener = (ArgentumListener)te;
            jTimeout.setText(te.getPropertyAsString(ArgentumListener.timeout));
            jOutputFile.setText(te.getPropertyAsString(ArgentumListener.outputFileName));
            jPercentiles.setText(te.getPropertyAsString(ArgentumListener.percentiles));
            jTimePeriods.setText(te.getPropertyAsString(ArgentumListener.timePeriods));
        }
    }

    @Override
    public void modifyTestElement(TestElement te) {
        super.configureTestElement(te);
        if(te instanceof ArgentumListener) {
            ArgentumListener listener = (ArgentumListener)te;
            try{
                listener.setTimeout(Integer.parseInt(jTimeout.getText()));
            } catch (NumberFormatException nfe) {
                listener.setTimeout(10);
            }
            listener.setOutputFileName(jOutputFile.getText());
            listener.setPercentiles(jPercentiles.getText());
            listener.setTimePeriods(jTimePeriods.getText());
        }
    }

    private void init() {
        setLayout(new BorderLayout(0, 5));
        setBorder(makeBorder());

        jGeneralPanel = new JPanel(new GridBagLayout());
        jGeneralPanel.setAlignmentX(0);
        jGeneralPanel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createEtchedBorder(),
                ""));
        GridBagConstraints labelConstraints = new GridBagConstraints();
        labelConstraints.anchor = GridBagConstraints.FIRST_LINE_END;

        GridBagConstraints editConstraints = new GridBagConstraints();
        editConstraints.anchor = GridBagConstraints.FIRST_LINE_START;
        editConstraints.weightx = 1.0;
        editConstraints.fill = GridBagConstraints.HORIZONTAL;

        editConstraints.insets = new java.awt.Insets(2, 0, 0, 0);
        labelConstraints.insets = new java.awt.Insets(2, 0, 0, 0);

        addToPanel(jGeneralPanel, labelConstraints, 0, 0, new JLabel("Timeout, [s]: ", JLabel.LEFT));
        addToPanel(jGeneralPanel, editConstraints, 1, 0, jTimeout = new JTextField(6));

        addToPanel(jGeneralPanel, labelConstraints, 0, 1, new JLabel("Output File: ", JLabel.LEFT));
        addToPanel(jGeneralPanel, editConstraints, 1, 1, jOutputFile = new JTextField(32));

        addToPanel(jGeneralPanel, labelConstraints, 0, 2, new JLabel("Percentiles: ", JLabel.LEFT));
        addToPanel(jGeneralPanel, editConstraints, 1, 2, jPercentiles = new JTextField(32));

        addToPanel(jGeneralPanel, labelConstraints, 0, 3, new JLabel("Time periods, [ms]: ", JLabel.LEFT));
        addToPanel(jGeneralPanel, editConstraints, 1, 3, jTimePeriods = new JTextField(32));

        JTextArea info = new JTextArea();
        info.setEditable(false);
        info.setWrapStyleWord(true);
        info.setOpaque(false);
        info.setLineWrap(true);
        info.setColumns(20);

        JScrollPane jScrollPane1 = new javax.swing.JScrollPane();
        jScrollPane1.setViewportView(info);
        jScrollPane1.setBorder(null);

        info.setText("https://github.com/sch1z0phren1a/jmeter-argentum");

        add(jScrollPane1, BorderLayout.CENTER);
        add(jGeneralPanel, BorderLayout.CENTER);
    }

    private void addToPanel(JPanel panel, GridBagConstraints constraints, int col, int row, JComponent component) {
        constraints.gridx = col;
        constraints.gridy = row;
        panel.add(component, constraints);
    }
}
