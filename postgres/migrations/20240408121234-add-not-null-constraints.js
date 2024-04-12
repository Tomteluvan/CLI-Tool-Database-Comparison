'use strict';

/** @type {import('sequelize-cli').Migration} */
module.exports = {
  async up (queryInterface, Sequelize) {
    /**
     * Add altering commands here.
     *
     * Example:
     * await queryInterface.createTable('users', { id: Sequelize.INTEGER });
     */
    await queryInterface.changeColumn('devices', 'id', {
      type: Sequelize.UUID,
      allowNull: false    
    });

    await queryInterface.changeColumn('devices', 'serial', {
      type: Sequelize.TEXT,
      allowNull: false    
    });

    await queryInterface.changeColumn('devices', 'type', {
      type: Sequelize.TEXT,
      allowNull: false    
    });

    await queryInterface.changeColumn('devices', 'sub_type', {
      type: Sequelize.TEXT,
      allowNull: false    
    });
  },

  async down (queryInterface, Sequelize) {
    /**
     * Add reverting commands here.
     *
     * Example:
     * await queryInterface.dropTable('users');
     */
  }
};
