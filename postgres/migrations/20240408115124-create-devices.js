'use strict';
/** @type {import('sequelize-cli').Migration} */
module.exports = {
  async up(queryInterface, Sequelize) {
    await queryInterface.createTable('devices', {
      id: {
        type: Sequelize.UUID
      },
      serial: {
        type: Sequelize.TEXT
      },
      type: {
        type: Sequelize.TEXT
      },
      sub_type: {
        type: Sequelize.TEXT
      }
    });
  },
  async down(queryInterface, Sequelize) {
    await queryInterface.dropTable('devices');
  }
};